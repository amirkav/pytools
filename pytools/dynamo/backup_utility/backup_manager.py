#!/usr/bin/env python

import os
from typing import Any, Dict, Optional

from botocore.exceptions import ClientError
from dynamoquery.data_table import DataTable

from pytools.common import yaml_tools
from pytools.common.logger import Logger
from pytools.aws.s3_connect import S3Connect


class BackupManagerError(Exception):
    """Base exception for `ConfigBackupManager`"""


class BackupManager:
    """
    Manager to backup and restore DynamoDB table data from/to S3.

    Can backup and restore set values as well.

    Arguments:
        bucket_name -- S3 bucket name that stores backups
    """

    def __init__(
        self, s3_bucket: str, aws_region: Optional[str] = None, env: Optional[str] = None
    ) -> None:
        self._env = env or os.environ["ENV"]
        self._aws_region = aws_region or os.environ["AWS_REGION"]
        self._s3_connect = S3Connect(aws_region=self._aws_region)
        self._logger = Logger(__name__)
        self._s3_bucket = s3_bucket

    def backup(self, s3_key: str, data_table: DataTable) -> None:
        """
        Save DynamoDB table data to S3 backup.

        Arguments:
            s3_key -- S3 key to create.
            data_table -- DataTable with records to backup.

        Raises:
            ConfigBackupManagerError -- If Dynamo or S3 query fails.
        """
        self._logger.info(f"Creating backup for {s3_key}...")

        self._logger.info(f"Backing up {data_table.max_length} records")
        records = data_table.get_records()
        yaml_data = yaml_tools.dump([self._annotate_data(dict(record)) for record in records])
        self._logger.info(f"Backup to S3 {self._s3_bucket}/{s3_key}")
        try:
            self._s3_connect.upload_data_to_s3(
                bucket_name=self._s3_bucket, key=s3_key, data=yaml_data
            )
        except ClientError as e:
            raise BackupManagerError(
                f"Unable to save data to S3: {e.response['Error']['Message']}"
            ) from e

    def backup_exists(self, s3_key: str) -> bool:
        s3_keys = self._s3_connect.list_s3_keys(self._s3_bucket)
        return s3_key in s3_keys if s3_keys is not None else False

    def restore(self, s3_key: str) -> DataTable:
        """
        Restore DynamoDB table data from S3 backup.
        Remove fields that are not present in backup.

        Arguments:
            s3_key -- S3 key to read backup.

        Raises:
            BackupManagerError -- If Dynamo or S3 query fails.
        """
        self._logger.info(f"Getting items from S3 {self._s3_bucket}/{s3_key}")
        try:
            yaml_data = self._s3_connect.read_data_from_s3(bucket_name=self._s3_bucket, key=s3_key)
        except ClientError as e:
            raise BackupManagerError(
                f"Unable to read data from S3: {e.response['Error']['Message']}"
            ) from e
        if not yaml_data:
            raise BackupManagerError("Unable to read data from S3")

        data = yaml_tools.load(yaml_data.decode("utf-8"))
        result: DataTable = DataTable()
        for record in data:
            result.add_record(self._deannotate_data(record))

        return result

    def _annotate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            if isinstance(value, set):
                key = f"{key}__set"
                value = sorted(value)
            if isinstance(value, dict):
                value = self._annotate_data(value)
            result[key] = value
        return result

    def _deannotate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        result = {}
        for key, value in data.items():
            if key.endswith("__set"):
                key = key.rsplit("__", 1)[0]
                value = set(value)
            if isinstance(value, dict):
                value = self._deannotate_data(value)
            result[key] = value
        return result
