import logging
from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar

from dynamoquery.data_table import DataTable
from dynamoquery.sentinel import SentinelValue

from pytools.common.class_utils import cached_property
from pytools.app.configs import Configs
from pytools.dynamo.base.partitioned_dynamo_record import PartitionedDynamoRecord
from pytools.dynamo.base.dynamo_table_base import DynamoTableBase, DynamoTableError
from pytools.dynamo.partitioning.partition_manager import PartitionManager

_RecordType = TypeVar("_RecordType", bound=PartitionedDynamoRecord)

# There are two modes for the subclass
# 1) If the subclass is initialized without `{config}` - it will act as a table level manager
# 2) If the subclass is initialized without `{config}` - it will act as a project level manager
class PartitionedDynamoTableBase(Generic[_RecordType], DynamoTableBase[_RecordType], ABC):
    NOT_SET = SentinelValue("NOT_SET")

    default_partition_count: int = 1
    # Changing `{altitude_hash_columns}` value will change the hashes for existing records. If it
    # still needs to be done, please make sure that you clear the table and reinsert all the
    # existing records, otherwise, the hashes of the existing records will never match and
    # and that will result in duplicate entries.
    altitude_hash_columns: List[str] = ["platform", "platform_id"]

    def __init__(
        self,
        env: Optional[str] = None,
        aws_region: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(logger=logger, env=env, aws_region=aws_region)
        self._config: Optional[Config] = None

    @property
    @abstractmethod
    def table_name(self) -> str:
        pass

    @property
    def config(self) -> Config:
        if not self._config:
            raise DynamoTableError(
                "Table manager cannot access project records, use project manager"
            )
        return self._config

    @cached_property
    def partition_manager(self) -> PartitionManager:
        return PartitionManager(partition_names=set(self.partition_names))

    @property
    def project_id(self) -> str:
        return self.config.project_name

    @property
    def partition_count(self) -> int:
        """
        Number of table partitions.
        """
        return self.default_partition_count

    @cached_property
    def partition_names(self) -> List[str]:
        return [f"{self.project_id}_{i}" for i in range(1, self.partition_count + 1)]

    def normalize_record(self, record: _RecordType) -> _RecordType:
        record.sanitize(partition_manager=self.partition_manager, project_id=self.project_id)
        return record

    def _validate_partition_key(self, partition_key: str) -> None:
        if partition_key not in self.partition_names:
            raise DynamoTableError(
                f"Invalid partition_key={partition_key} for project={self.project_id}"
            )

    # Table/Project Level Operations #

    def clear_partition(self, *partition_keys: str) -> None:
        """
        Delete partition records from table.

        Arguments:
            partition_keys -- Partition key value.
        """
        for partition_key in partition_keys:
            self._validate_partition_key(partition_key)
            super().clear_table(partition_key=partition_key, sort_key_prefix=self.sort_key_prefix)

    def backup_partition(self, *partition_keys: str) -> None:
        """
        Backup partition records to S3.

        Arguments:
            partition_keys -- Partition key value.
        """
        for partition_key in partition_keys:
            self._validate_partition_key(partition_key)
            s3_key = f"{self.table_name}-{partition_key}.yml"
            data_table = DataTable[_RecordType]()
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
            self._validate_partition_key(partition_key)
            s3_key = f"{self.table_name}-{partition_key}.yml"
            backup_exists = self._backup_manager.backup_exists(s3_key)
            if not backup_exists:
                raise DynamoTableError(f"Backup {s3_key} not found. Run .backup() first.")

            self.clear_table(sort_key_prefix=self.sort_key_prefix)
            data_table = self._backup_manager.restore(s3_key)
            self.batch_upsert(data_table)

    # Project Level Operations #

    def clear_project(self) -> None:
        """
        Delete all project records from table.
        """
        self.clear_partition(*self.partition_names)

    def backup_project(self) -> None:
        """
        Backup project records to S3.
        """
        self.backup_partition(*self.partition_names)

    def restore_project(self) -> None:
        """
        Restore project records from S3 backup.

        Deletes all project records with from the table.
        """
        self.restore_partition(*self.partition_names)
