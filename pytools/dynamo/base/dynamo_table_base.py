import logging
import os
from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, Set, TypeVar

from boto3.session import Session as Boto3Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from dynamoquery import DynamoDictClass
from dynamoquery.data_table import DataTable
from dynamoquery.dynamoquery_main import DynamoQuery
from dynamoquery.dynamo_table import DynamoTable, DynamoTableError
from dynamoquery.dynamo_table_index import DynamoTableIndex

from pytools.aws.boto3_session_generator import Boto3SessionGenerator
from pytools.aws.dynamo_autoscale_helper import DynamoAutoscaleHelper
from pytools.aws.dynamo_connect import DynamoConnect
from pytools.common.logger import Logger

__all__ = ("DynamoTableBase", "DynamoTableIndex")

_RecordType = TypeVar("_RecordType", bound=DynamoDictClass)


class LimitedDynamoQuery(DynamoQuery):
    MAX_LIMIT = 20


class DynamoTableBase(Generic[_RecordType], DynamoTable[_RecordType], ABC):
    NULL: str = "NULL"

    # Table physical attributes (these must match what we have on AWS)
    partition_key_name: str = "pk"
    sort_key_name: Optional[str] = "sk"

    DEFAULT_CAPACITY: int = DynamoAutoscaleHelper.SCALE_MIN_CAPACITY
    SCALED_MAX_CAPACITY: int = DynamoAutoscaleHelper.SCALE_MAX_CAPACITY

    read_capacity_units: int = 50
    write_capacity_units: int = 10

    skip_auto_scaling: bool = False

    dynamoquery_class = LimitedDynamoQuery

    endpoint_url: Optional[str] = None

    def __init__(
        self,
        env: Optional[str] = None,
        aws_region: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        boto3_session: Optional[Boto3Session] = None,
        dynamo_connect: Optional[DynamoConnect] = None,
    ) -> None:
        super().__init__(logger=logger or Logger.for_object(self))
        self._env = env or os.environ["ENV"]
        self._aws_region = aws_region or os.environ["AWS_REGION"]
        self.session = (
            boto3_session
            or Boto3SessionGenerator(aws_region=self._aws_region).generate_default_session()
        )
        self.dynamo_connect = dynamo_connect or DynamoConnect(boto3_session=self.session)

    @property
    def resource(self) -> Any:
        return self.dynamo_connect.resource

    @property
    def app_autoscaling_client(self) -> BaseClient:
        return self.dynamo_connect.app_autoscaling_client

    @property
    def autoscale_helper(self) -> DynamoAutoscaleHelper:
        return self.dynamo_connect.autoscale_helper

    @property
    def table(self) -> Any:
        return self.resource.Table(self.table_name)

    def get_partition_key(self, record: _RecordType) -> Any:
        """
        Defines the mapping between the record and the partition key on DynamoDB.
        Use when the attribute key in the data does not match the partition key name on DynamoDB.
        """
        raise DynamoTableError(
            f"{self.__class__.__name__}.get_partition_key method is missing,"
            f" cannot get {self.partition_key_name} for {record}"
        )

    def get_sort_key(self, record: _RecordType) -> Any:
        """
        Defines the mapping between the record and the sort key on DynamoDB.
        Use when the attribute key in the data does not match the partition key name on DynamoDB.
        """
        raise DynamoTableError(
            f"{self.__class__.__name__}.get_sort_key method is missing,"
            f" cannot get {self.sort_key_name} for {record}"
        )

    @property
    @abstractmethod
    def table_name(self) -> str:
        pass

    def create_table(self) -> None:
        """
        Create DynamoDB table and register autoscaling.

        Waits until table exists.
        """
        global_secondary_indexes = [
            i.as_global_secondary_index() for i in self.global_secondary_indexes
        ]

        super().create_table()
        self._logger.info(f"Table {self.table_name} create initiated")
        self.wait_until_exists()
        self._logger.info(f"Table {self.table_name} created")

        if self.skip_auto_scaling:
            return

        self.autoscale_helper.register_auto_scaling(
            self.table_name,
            iter(global_secondary_indexes),
            min_capacity=self.DEFAULT_CAPACITY,
            max_capacity=self.SCALED_MAX_CAPACITY,
        )
        self._logger.info(f"Table {self.table_name} autoscale registered")

    def delete_table(self) -> None:
        """
        Delete the table from DynamoDB and deregister auto scaling

        """
        global_secondary_indexes = [
            i.as_global_secondary_index() for i in self.global_secondary_indexes
        ]

        try:
            self.autoscale_helper.deregister_auto_scaling(
                self.table_name,
                iter(global_secondary_indexes),
            )
            self._logger.info(f"Table {self.table_name} autoscale deregistered")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ObjectNotFoundException":
                raise

        super().delete_table()

    def backup(self) -> None:
        """
        Backup records to S3.
        """
        s3_key = f"{self.table_name}.yml"
        data_table = DataTable[_RecordType]()
        for record in self.scan():
            data_table.add_record(record)
        self._backup_manager.backup(s3_key=s3_key, data_table=data_table)

    def restore(self) -> None:
        """
        Restore records from S3 backup.

        Deletes all records starting with `sort_key_prefix` from the table.
        """
        s3_key = f"{self.table_name}.yml"

        backup_exists = self._backup_manager.backup_exists(s3_key)
        if not backup_exists:
            raise DynamoTableError(f"Backup {s3_key} not found. Run .backup() first.")

        self.clear_table(sort_key_prefix=self.sort_key_prefix)
        data_table = self._backup_manager.restore(s3_key)
        self.batch_upsert(data_table)

    def get_optional_update_keys(self, record: _RecordType) -> Set[str]:
        return {k for k, v in record.items() if v == self.NULL}

    def clear_partition(self, *partition_keys: str) -> None:
        """
        Delete partition records from table.

        Arguments:
            partition_keys -- Partition key value.
        """
        for partition_key in partition_keys:
            self.clear_table(partition_key=partition_key, sort_key_prefix=self.sort_key_prefix)

    def backup_partition(self, *partition_keys: str) -> None:
        """
        Backup partition records to S3.

        Arguments:
            partition_keys -- Partition key value.
        """
        for partition_key in partition_keys:
            s3_key = f"{self.table_name}-{partition_key}.yml"
            data_table = DataTable(record_class=self.record_class)
            for record in self.query(partition_key):
                data_table.add_record(record)
            self._backup_manager.backup(s3_key=s3_key, data_table=data_table)

    def restore_partition(self, *partition_keys: str) -> None:
        """
        Restore partition records from S3 backup.

        Deletes all records with `partition_key` from the table.

        Arguments:
            partition_keys -- Partition key value.
        """
        for partition_key in partition_keys:
            s3_key = f"{self.table_name}-{partition_key}.yml"
            backup_exists = self._backup_manager.backup_exists(s3_key)
            if not backup_exists:
                raise DynamoTableError(f"Backup {s3_key} not found. Run .backup() first.")

            self.clear_table(sort_key_prefix=self.sort_key_prefix)
            data_table = self._backup_manager.restore(s3_key)
            self.batch_upsert(data_table)

    def clear_records(self) -> None:
        """
        Delete all records managed by current table manager.

        Deletes only records with sort key starting with `sort_key_prefix`.
        """
        super().clear_table(sort_key_prefix=self.sort_key_prefix)
