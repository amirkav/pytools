from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Iterable, Optional, cast

from pytools.aws.boto3_session_generator import Boto3Session, Boto3SessionGenerator
from pytools.aws.sts_connect import StsConnect
from pytools.common.class_utils import cached_property
from pytools.common.datetime_utils import UNIX_DATETIME_FORMAT
from pytools.common.dict_utils import get_nested_item

from pytools.aws.dynamo_connect import DynamoConnect
from pytools.common.logger import Logger


# TODO: Write a __repr__ method for this class. Use the "backup" method to create a YAML string and send to output.


class Configs:
    """
    An interface to the application's configs.
    Acts as a resource identifier for different entities (customers, projects, etc.).
    It can be thought of a static state machine, where the actual state is stored in DynamoDB,
    and is loaded on demand via this class.
    For a more dynamic state machine, see `pytools.app.data_state_table`.

    For the details of DynamoDB table and how it is set up, see `pytools.dynamo.configs_table`.

    Raises:
        KeyError -- If `env` or `aws_region` are not set and not exist in ENV.
    """

    TTL = 60  # Cache DynamoDB record for this many seconds.

    # This must match the DynamoTableBase.table_name property and the DynamoDB table name.
    table_name = "Configs"

    def __init__(
        self,
        id: str,
        env: Optional[str] = None,
        aws_region: Optional[str] = None,
        app_name: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
        suffix: Optional[str] = None,
    ) -> None:
        self._id = id
        self._env = env or os.environ["ENV"]
        self._aws_region = aws_region or os.environ["AWS_REGION"]
        self._app_name = app_name or os.environ["APP_NAME"]
        self._suffix = suffix
        self._logger = Logger(__name__)
        self._configs: Optional[Dict[str, Any]] = None
        self._configs_expires_at: float = 0

        self._boto3_session = (
            boto3_session
            or Boto3SessionGenerator(
                default_profile=self.env, aws_region=self.aws_region
            ).generate_default_session()
        )
        self._dyn_connect = DynamoConnect(
            aws_region=self.aws_region, boto3_session=self._boto3_session
        )
        self._sts_connect = StsConnect(
            aws_region=self.aws_region, boto3_session=self._boto3_session
        )

    @property
    def boto3_session(self) -> Boto3Session:
        return self._boto3_session

    def _get_configs(self, id: str) -> Dict[str, Any]:
        """
        Read config record from `configs` DynamoDB table and return it without caching.
        """
        self._logger.debug("Reading configs record from dynamodb.")
        return cast(
            Dict[str, Any],
            self._dyn_connect.get_item(Configs.table_name, key_dict={"pk": id}),
        )

    def get_attribute(
        self,
        configs: Dict[str, Any],
        attribute_path: Iterable[Any],
        raise_errors: bool = False,
    ) -> Any:
        """
        Get atrribute value from Configs table.

        Arguments:
            configs -- Target config.
            attribute_path -- Attribute path as a list.
            raise_errors -- Whether to raise ValueError if attribute does not exist
        """
        try:
            return get_nested_item(configs, attribute_path, raise_errors=True)
        except (AttributeError, KeyError) as e:
            message = f"Configs path not found: {'/'.join(map(str, attribute_path))}"
            if raise_errors:
                raise ValueError(message) from e
            self._logger.debug(message)
            return None

    @staticmethod
    def typed_getenv(
        env_var: str, parser: Optional[Callable[[str], Any]] = None, default: Any = None
    ) -> Any:
        value = os.getenv(env_var)
        if value is None:
            return default
        if parser:
            value = parser(value)
        return value

    @cached_property(ttl=TTL)
    def configs(self) -> Dict[str, Any]:
        """
        Return config record. Reads from `configs` DynamoDB table and caches for up to
        `Configs.TTL` seconds (60 seconds).
        """
        return self._get_configs(id=self.id)

    # Core item configuration
    # =========================================================================

    @property
    def id(self) -> str:
        return self._id

    @property
    def app_name(self) -> str:
        return self._app_name

    @property
    def env(self) -> str:
        return self._env

    @property
    def local(self) -> bool:
        return self.env == "local"

    @property
    def aws_region(self) -> str:
        return self._aws_region

    @property
    def suffix(self) -> str:
        if not self._suffix:
            self._suffix = os.environ.get("SUFFIX") or "01"
        return self._suffix

    @cached_property
    def aws_account_id(self) -> str:
        return self._sts_connect.get_account_id()

    # Infrastructure
    # =========================================================================

    @property
    def kms_key_arn(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "kms_key_arn"])

    @property
    def kms_key_id(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "kms_key_id"])

    @property
    def s3_bucket(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "s3_bucket"])

    # Database configuration
    # =========================================================================

    @property
    def db_instance_id(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "db_instance_id"])

    @property
    def db_iam_auth(self) -> bool:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "db_iam_auth"]) == "1"

    @property
    def readonly_user(self) -> str:
        return f"{self.id}_{self.env}_ro"

    @property
    def readwrite_user(self) -> str:
        return f"{self.id}_{self.env}_rw"

    @property
    def readonly_pass_parameter(self) -> str:
        return f"/{self.app_name}/{self.env}/{self.aws_region}/{self.id}/db_readonly_pass"

    @property
    def readwrite_pass_parameter(self) -> str:
        return f"/{self.app_name}/{self.env}/{self.aws_region}/{self.id}/db_readwrite_pass"

    @property
    def master_pass_parameter(self) -> str:
        return f"/{self.app_name}/{self.env}/{self.aws_region}/{self.db_instance_id}/db_master_pass"

    @property
    def rds_cert_parameter(self) -> str:
        return f"/{self.app_name}/{self.env}/{self.aws_region}/{self.db_instance_id}/rds_cert"

    # Bastion host
    # TODO: abstract these to a separate record dedicated to the bastion host, and only reference it in each individual project record
    # -------------------------------------------------------------------------
    @property
    def db_bastion_username(self) -> str:
        return "ec2-user"

    @property
    def db_bastion_host(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "db_bastion_host"])

    @property
    def db_bastion_port(self) -> Optional[int]:
        return int(self.get_attribute(self.configs, [self.env, self.aws_region, "db_bastion_port"]))

    @property
    def db_name(self) -> str:
        return f"{self.id}_{self.env}"

    # PostgreSQL database
    # -------------------------------------------------------------------------
    @cached_property
    def db_configs(self) -> Dict[str, Any]:
        """
        Configs for the database instance.
        """
        return self._get_configs(self.db_instance_id)

    @property
    def pg_db_endpoint(self) -> str:
        return self.get_attribute(self.db_configs, [self.env, self.aws_region, "pg_db_endpoint"])

    @property
    def pg_db_ro_endpoint(self) -> str:
        return self.get_attribute(self.db_configs, [self.env, self.aws_region, "pg_db_ro_endpoint"])

    @property
    def pg_db_api_endpoint(self) -> str:
        return self.get_attribute(
            self.db_configs, [self.env, self.aws_region, "pg_db_api_endpoint"]
        )

    @property
    def pg_db_cluster_id(self) -> str:
        return self.get_attribute(self.db_configs, [self.env, self.aws_region, "pg_db_cluster_id"])

    @property
    def pg_db_instance_id(self) -> str:
        return self.get_attribute(self.db_configs, [self.env, self.aws_region, "pg_db_instance_id"])

    @property
    def pg_db_port(self) -> int:
        value = self.get_attribute(self.db_configs, [self.env, self.aws_region, "pg_db_port"])
        return int(value) if value else 5432

    @property
    def pg_master_user(self) -> str:
        return "postgres"


def main():

    configs = Configs("pgsb")
    print(configs.aws_account_id)
    print(configs.pg_db_endpoint)


if __name__ == "__main__":
    main()
