import hashlib
from typing import Any, List, Optional

from dynamoquery.dynamo_table import DynamoTableError

from pytools.dynamo.base.dynamo_record import DynamoRecord
from pytools.dynamo.partitioning.partition_manager import PartitionManager


class PartitionedDynamoRecord(DynamoRecord):
    # do not change HASH_COLUMNS, otherwise table has to be dropped
    HASH_COLUMNS: List[str] = []

    def get_hash(self) -> str:
        if not self.HASH_COLUMNS:
            raise DynamoTableError(f"{self._class_name}.HASH_COLUMNS cannot be empty")

        hash_values = [self[key] for key in self.HASH_COLUMNS]
        data_hash = hashlib.sha1("-".join(hash_values).encode()).hexdigest()
        return f"{self.SORT_KEY_PREFIX}{data_hash}"

    @DynamoRecord.sanitize_key("pk")
    def sanitize_key_pk(self, value: Optional[str], **kwargs: Any) -> Optional[str]:
        """
        Calculate and set `pk` dict key implicitly.

        Requires `partition_manager` in kwargs.

        - if `partition_manager` not in `kwargs`, returns `value`
        - if `value` is set and any of `HASH_COLUMNS` is not defined, returns `value`
        - if 'value' is falsy and any of `HASH_COLUMNS` is not defined, raises DynamoTableError
        - if 'value' is falsy and all `HASH_COLUMNS` are defined,
          calculates `pk` from `HASH_COLUMNS`
        - if `value` is set and all `HASH_COLUMNS` are defined,
          checks if value = expected, returns `value`
        - if value != expected, raises DynamoTableError

        Arguments:
            value -- `pk` value to set
            kwargs -- Should contain `partition_manager`

        Returns:
            Sanitized `pk` value.
        """
        partition_manager: Optional[PartitionManager] = kwargs.get("partition_manager")
        if not partition_manager:
            return value

        has_hash_columns = all(self.get(i) for i in self.HASH_COLUMNS)
        if not has_hash_columns:
            return value

        data_hash = self.get_hash()
        expected = partition_manager.get_partition(key=data_hash)

        if value is not None and value != expected:
            raise DynamoTableError(
                f"Invalid record with pk={value}. Please don't update hash values"
            )

        return expected

    @DynamoRecord.sanitize_key("sk")
    def sanitize_key_sk(self, value: Optional[str], **_kwargs: Any) -> Optional[str]:
        """
        Calculate and set `sk` dict key implicitly.

        - if `value` is set and any of `HASH_COLUMNS` is not defined, returns `value`
        - if 'value' is falsy and any of `HASH_COLUMNS` is not defined, raises DynamoTableError
        - if 'value' is falsy and all `HASH_COLUMNS` are defined,
          calculates `sk` from `HASH_COLUMNS`
        - if `value` is set and all `HASH_COLUMNS` are defined,
          checks if value = expected, returns `value`
        - if value != expected, raises DynamoTableError

        Arguments:
            value -- `sk` value to set

        Returns:
            Sanitized `sk` value.
        """
        has_hash_columns = all(self.get(i) for i in self.HASH_COLUMNS)

        if not has_hash_columns:
            return value

        expected = self.get_hash()
        if value is not None and value != expected:
            raise DynamoTableError(
                f"Invalid record with sk={value}, expected={expected}."
                " Please don't update hash values directly"
            )

        return expected
