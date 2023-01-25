# TODO: update file references. This file used to be called `configs_table.py`.

from dynamoquery.dynamo_table_index import DynamoTableIndex

from pytools.dynamo.configs_table.configs_record import ConfigsRecord
from pytools.dynamo.base.dynamo_table_base import DynamoTableBase


class ConfigsAdmin(DynamoTableBase[ConfigsRecord]):
    """
    Class for Configs table.
    """

    record_class = ConfigsRecord
    partition_key_name = "pk"
    sort_key_name = None

    # gsi_status = DynamoTableIndex(
    #     name="status-index", partition_key_name="status", sort_key_name=partition_key_name
    # )

    # gsi_gsuite_customer_id = DynamoTableIndex(
    #     name="gsuite-customer-id-index",
    #     partition_key_name="gsuite_customer_id",
    #     sort_key_name=partition_key_name,
    #     projection=["status"],
    # )

    # global_secondary_indexes = [gsi_status, gsi_gsuite_customer_id]

    @property
    def table_name(self) -> str:
        return "Configs"

    def get_partition_key(self, record: ConfigsRecord) -> str:
        return record["id"]
