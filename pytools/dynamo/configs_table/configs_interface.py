from typing import Any, Dict, Optional

from botocore.exceptions import ClientError

from pytools.common import dict_utils, yaml_utils
from pytools.common.class_utils import cached_property
from pytools.dynamo.configs_table.configs_admin import ConfigsAdmin
from pytools.dynamo.configs_table.configs_record import ConfigsRecord
from pytools.common.logger import Logger
from pytools.aws.s3_connect import S3Connect
from pytools.app.configs import Configs


class ConfigsInterfaceError(Exception):
    "Base exception for `ConfigsInterface`"


# TODO: update logging on this file. Lots of useless logs.


class ConfigsInterface(ConfigsAdmin):
    _logger: Logger

    def __init__(
        self,
        aws_region: Optional[str] = None,
        env: Optional[str] = None,
        s3_connect: Optional[S3Connect] = None,
    ) -> None:
        """
        Args:
            id: The id of the configs record.
            backup_s3_bucket: The S3 bucket that holds configs backup data.
            aws_region: The AWS region to use.
            env: The environment to use.
            s3_connect: The S3Connect object to use.

        """
        super().__init__(env=env, aws_region=aws_region)
        self.s3_connect = s3_connect or S3Connect(aws_region=self._aws_region)

    ############### Backup data load and dump ###############
    def default_backup_s3_bucket(self, id: str) -> str:
        """
        Returns the S3 bucket that holds configs backup data.
        If the bucket is not specified by the caller, the bucket is retrieved from the configs table.
        Returns:
            A string containing the S3 key of the configs backup yaml file.
        """

        return Configs(id=id, env=self._env, aws_region=self._aws_region).s3_bucket

    def default_backup_s3_key(self, id: str) -> str:
        """
        Returns the S3 key of the yaml file that stores record configs backup data.
        Returns:
            A string containing the S3 key of the configs backup yaml file.
        """
        return f"configs_table/{id}_{self._env}.yml"

    ############### Functions for backup ###############
    def load_data_from_backup(
        self,
        id: str,
        override_s3_backup_bucket: Optional[str] = None,
        override_s3_backup_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        The Configs record to use as a starting point for creating a new record or updating an existing one
        Either use the record supplied by the constructor or retrieve it from S3.
        """
        if not override_s3_backup_bucket:
            backup_bucket = self.default_backup_s3_bucket(id=id)

        if not override_s3_backup_key:
            backup_key = self.default_backup_s3_key(id=id)

        self._logger.debug(f"Getting items from S3 {backup_bucket}/{backup_key}")
        try:
            yaml_data = self.s3_connect.read_data_from_s3(bucket_name=backup_bucket, key=backup_key)
        except ClientError as e:
            raise ConfigsInterfaceError(
                f"Unable to read config data from "
                f"{backup_bucket}/{backup_key}: "
                f"{e.response['Error']['Message']}"
            ) from e

        if not yaml_data:
            raise ConfigsInterfaceError(f"Empty configs file {backup_bucket}/{backup_key}.")
        return yaml_utils.load(yaml_data.decode("utf-8"))

    def backup(
        self,
        override_s3_backup_bucket: Optional[str] = None,
        override_s3_backup_key: Optional[str] = None,
    ) -> None:
        """
        Creates a configs backup as a YAML file, and uploads it to the bucket defined by backup_bucket.
        """
        if not override_s3_backup_bucket:
            backup_bucket = self.default_backup_s3_bucket

        if not override_s3_backup_key:
            backup_key = self.default_backup_s3_key

        self._logger.info(f"Creating a backup configs for {id} entry...")
        configs_data = self.get_configs()

        if not configs_data:
            raise ConfigsInterfaceError(f"Config not found for id {id}.")

        self._logger.json(f"Backup data: {configs_data}")
        yaml_data = yaml_utils.dump(self._annotate_data(configs_data))
        self._logger.info(f"Backup record with id {id} to S3 " f"{backup_bucket}/{backup_key}")
        try:
            self.s3_connect.upload_data_to_s3(
                bucket_name=backup_bucket,
                key=backup_key,
                data=yaml_data,
            )
        except ClientError as e:
            raise ConfigsInterfaceError(
                f"Unable to save config data to S3: {e.response['Error']['Message']}"
            ) from e

    def restore_from_backup(self) -> None:
        """
        Resets configs entries to original/default values from backup file.
        """
        configs_data = self._deannotate_data(data=self.load_data_from_backup())
        self.clear()
        configs_record = ConfigsRecord(pk=id)
        configs_record.update(configs_data)
        self.upsert_record(configs_record)

    ############### Functions for creating and updating records ###############
    # TODO: Are the next two functions the same? If so, remove one of them.
    def upsert_attributes(self, id: str, attrs: Dict) -> None:
        """
        Upserts a configs table record, given a record id and a dict of attributes to update.

        # TODO: verify the following comment. Given the use of `recursive_merge_dict`, it seems that this would create new paths.
        The attrs given should be existing paths. It does not create new paths.

        ```python
        attrs = {
            "dev": {
                "us-west-2": {
                    "my_new_item": "my_new_item_value"
                }
            }
        }
         #  Updates key for my_new_item
        configs_interface.upsert_attributes(attrs)
        ```
        Args:
            attrs -- Attributes to upsert
        """
        if not attrs:
            return

        configs_record = self.get_configs() or ConfigsRecord(pk=id)
        configs_record.update(dict_utils.recursive_merge_dict(configs_record, attrs))
        self.upsert_record(configs_record)

    def upsert_configs_record(self, id: str, configs_data: Dict[str, Any]) -> None:
        """
        Update an existing config record with `configs_data`, or create a new record if it doesn't exist.
        Acts similar to UPSERT in SQL.

        # TODO: Study `upsert_record` function, see if it keeps existing paths if the upsert data does not include them (i.e., whether it is an override or an update or a true upser method).
        Supports partial update.

        Arguments:
            configs_data -- Updated config data.
        """
        configs_record = ConfigsRecord({**configs_data, "pk": id})
        self.upsert_record(configs_record)

    ############### Functions for reading from Dynamo and S3 ###############
    def get_configs(self) -> Optional[ConfigsRecord]:
        """
        Gets a configs data record for a specific id
        Returns:
            Configs record representing the record

        """
        # self._logger.info(f"Getting {id} config")
        record = self.get_record(ConfigsRecord(id=id))
        return record

    def clear(self) -> None:
        """
        Clears config entries for the record, deletes record from configs.
        """
        self._logger.info(f"Clearing entry with id: {id}")
        self.delete_record(ConfigsRecord(id=id))

    def _annotate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Serializes and annotates *Set* values before dumping to yaml.
        Set values will be stored as sorted lists with the key as `{key_name}__set`.

        Args:
            data: JSON serializable data

        Returns:
            Annotated data
        """
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
            # If clear_fields is set, update `values` to NOT_SET if its not a dictionary type.
            if isinstance(value, dict):
                value = self._deannotate_data(value)
            result[key] = value
        return result
