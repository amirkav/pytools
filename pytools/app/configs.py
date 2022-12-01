from __future__ import annotations

import os
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, cast

from pytools.aws.boto3_session_generator import Boto3Session, Boto3SessionGenerator
from pytools.common.class_tools import cached_property
from pytools.common.datetime_utils import UNIX_DATETIME_FORMAT
from pytools.common.dict_utils import get_nested_item

# from pytools.dynamo_connect import DynamoConnect
from pytools.common.logger import Logger


class Configs:
    """
    A state class to store and manage project resource identifiers.
    It loads its base data from 'configs' table on dynamodb.

    Raises:
        KeyError -- If `env` or `aws_region` are not set and not exist in ENV.
    """

    @dataclass
    class InternalDomain:
        name: str
        affiliate_level: int = 100

    TTL = 60  # Cache DynamoDB record for this many seconds.

    table_name = "configs"

    def __init__(
        self,
        pk: str,
        env: Optional[str] = None,
        aws_region: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
        suffix: Optional[str] = None,
    ) -> None:
        self._pk = pk
        self._env = env or os.environ["ENV"]
        self._aws_region = aws_region or os.environ["AWS_REGION"]
        self._aws_account = self.get_aws_account(self._env)
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

    @property
    def boto3_session(self) -> Boto3Session:
        return self._boto3_session

    def get_configs(self) -> Dict[str, Any]:
        """
        Read config record from `configs` DynamoDB table and return it without caching.
        """
        self._logger.debug("Reading configs record from dynamodb.")
        return cast(
            Dict[str, Any],
            self._dyn_connect.get_item(Configs.table_name, key_dict={"pk": self.pk}),
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
    def get_aws_account(env: str) -> str:
        account_id = {
            "local": "0",
            "dev": "??",
            "staging": "??",
            "prod": "??",
            "master": "??",
        }.get(env)
        if account_id:
            return account_id
        raise ValueError(f"Cannot get aws_account for unknown env {env}")

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

    @property
    def configs(self) -> Dict[str, Any]:
        """
        Return config record. Reads from `configs` DynamoDB table and caches for up to
        `Configs.TTL` seconds (60 seconds).
        """
        # This property cannot use the `@cached_property` decorator since that currently lacks
        # an explicit cache invalidation mechanism, which is needed by `_set_attr` and other methods
        # below.
        now = time.time()
        if self._configs is None or now >= self._configs_expires_at:
            self._configs = self.get_configs()
            self._configs_expires_at = now + self.TTL
        return self._configs

    @cached_property
    def ext_int_configs(self) -> Dict[str, Any]:
        return cast(
            Dict[str, Any],
            self._dyn_connect.get_item(
                Configs.table_name, key_dict={"pk": "_external_integrations"}
            ),
        )

    # Core item configuration
    # =========================================================================

    @property
    def pk(self) -> str:
        return self._pk

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

    @property
    def aws_account(self) -> str:
        return self._aws_account

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
        bucket_name = self.get_attribute(self.configs, [self.env, self.aws_region, "s3_bucket"])
        if bucket_name is None:
            return self.get_default_bucket_name()
        return bucket_name

    # Database configuration
    # =========================================================================

    @property
    def is_shared_rds_cluster(self) -> bool:
        return not self.local and (self.has_parent_project or self.is_shared_rsa)

    @property
    def is_shared_db(self) -> bool:
        return not self.local and self.has_parent_project

    @property
    def db_iam_auth(self) -> bool:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "db_iam_auth"]) == "1"

    @property
    def readonly_user(self) -> str:
        return (
            f"{self.pk}_{self.env}_ro" if self.is_shared_rds_cluster else f"{self.env}_readonlyuser"
        )

    @property
    def readwrite_user(self) -> str:
        return (
            f"{self.pk}_{self.env}_rw"
            if self.is_shared_rds_cluster
            else f"{self.env}_readwriteuser"
        )

    @property
    def default_master_pass_parameter(self) -> str:
        return f"/altitude/{self.env}/db_default_master_pass"

    @property
    def master_pass_parameter(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "master_pass_parameter"]
        )

    @property
    def readonly_pass_parameter(self) -> str:
        return f"/{self.project_prefix}/{self.env}/{self.aws_region}/db_readonly_pass"

    @property
    def readwrite_pass_parameter(self) -> str:
        return f"/{self.project_prefix}/{self.env}/{self.aws_region}/db_readwrite_pass"

    @property
    def rds_cert_parameter(self) -> str:
        return self.get_attribute(self.configs, [self.env, self.aws_region, "rds_cert_parameter"])

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
        return f"{self.project_prefix}_{self.env}"

    # PostgreSQL database
    # -------------------------------------------------------------------------

    @property
    def has_postgresql(self) -> bool:
        return bool(self.postgresql_endpoint)

    @property
    def postgresql_endpoint(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "postgresql_db_endpoint"]
        )

    @property
    def postgresql_db_ro_endpoint(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "postgresql_db_ro_endpoint"]
        )

    @property
    def postgresql_db_api_endpoint(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "postgresql_db_api_endpoint"]
        )

    @property
    def postgresql_db_cluster_id(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "postgresql_db_cluster_id"]
        )

    @property
    def postgresql_db_instance_id(self) -> str:
        return self.get_attribute(
            self.configs, [self.env, self.aws_region, "postgresql_db_instance_id"]
        )

    @property
    def postgresql_db_port(self) -> int:
        value = self.get_attribute(self.configs, [self.env, self.aws_region, "postgresql_db_port"])
        return int(value) if value else 5432

    @property
    def postgresql_master_user(self) -> str:
        return "postgres"
