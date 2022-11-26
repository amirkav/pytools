from __future__ import annotations

import binascii
import os
import time
import urllib.parse
from contextlib import contextmanager
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Union, cast

import psycopg2
import pymysql

from custody_py_tools.boto3_session_generator import Boto3Session, Boto3SessionGenerator
from custody_py_tools.default_type import Default, DefaultType
from custody_py_tools.param_store import ParamStore
from custody_py_tools.rds_connect import RdsConnect
from custody_py_tools.resources import CA_CERT_FILENAME, CA_CERTS_PATH

from .database_type import DatabaseType
from .errors import NoDatabaseOfSuchTypeError

if TYPE_CHECKING:
    from custody_py_tools.configs import Configs

from .ssh import PortForward, SSHTunnel  # pylint: disable=wrong-import-position


class Route:
    @classmethod
    def from_config(
        cls,
        configs: Configs,
        *,
        database_type: Optional[DatabaseType] = None,
        bastion_host: Union[Optional[str], DefaultType] = Default,
        boto3_session: Optional[Boto3Session] = None,
        param_store: Optional[ParamStore] = None,
        use_writer: bool = False,
        use_master: bool = False,
    ) -> Route:
        """
        Determine connection info based on project configuration.

        Arguments:
            configs -- `tools.configs.Configs` object that provides AWS region, database endpoints,
                database user names, and the database name.
            database_type -- (optional) `DatabaseType` value specifying the type of database to
                connect to. One of: `DatabaseType.MYSQL`, `DatabaseType.POSTGRESQL`.
                (default: `DatabaseType.MYSQL`)
            boto3_session -- (optional) `boto3.session.Session` object.
            param_store -- (optional) `tools.param_store.ParamStore` object that provides database
                passwords and CA SSL certificates.
            use_writer -- (optional) Boolean indicating whether write access is required.
                (default: False)
            use_master -- (optional) Boolean indicating whether master-user access is required.
                If `use_master` is specified, `use_writer` is ignored. (default: False)

        Returns:
            `Route` object describing where to connect and how to authenticate.
        """
        boto3_session = boto3_session or Boto3SessionGenerator().generate_default_session()
        param_store = param_store or ParamStore(configs.aws_region, boto3_session=boto3_session)

        database_type = database_type or DatabaseType[configs.database_type]
        host = database_type.switch(
            mysql=(configs.endpoint if use_writer or use_master else configs.db_ro_endpoint),
            postgresql=(
                configs.postgresql_endpoint
                if use_writer or use_master
                else configs.postgresql_db_ro_endpoint
            ),
        )
        port = database_type.switch(
            mysql=configs.db_port or 3306,
            postgresql=configs.postgresql_db_port or 5432,
        )
        if host is None:
            raise NoDatabaseOfSuchTypeError(
                f'Project "{configs.project_name}" has no {database_type.name} database'
            )

        database = configs.db_name
        # Master user cannot use IAM auth:
        iam_auth = configs.db_iam_auth if not use_master else False

        # User name:
        if use_master:
            user = database_type.switch(
                mysql=configs.master_user,
                postgresql=configs.postgresql_master_user,
            )
        elif use_writer:
            user = configs.readwrite_user
        else:
            user = configs.readonly_user

        # Password:
        password: Optional[str]
        if iam_auth:
            password = None
        else:
            if use_master:
                pass_parameter = configs.master_pass_parameter
            elif use_writer:
                pass_parameter = configs.readwrite_pass_parameter
            else:
                pass_parameter = configs.readonly_pass_parameter
            password = param_store.get_param(pass_parameter, True)

        route = cls(
            database_type=database_type,
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            iam_auth=iam_auth,
            expires_at=None,
            ssl=True,
            bastion_host=bastion_host,
            boto3_session=boto3_session,
        )
        if iam_auth:
            route.refresh_iam_token()
        return route

    @classmethod
    def from_mysql_env(cls) -> Route:
        try:
            return Route(
                database_type=DatabaseType.MYSQL,
                host=os.environ["MYSQL_HOST"],
                port=int(os.environ["MYSQL_TCP_PORT"]),
                user=os.environ["MYSQL_USER"],
                password=os.environ["MYSQL_PWD"],
                database=os.environ["MYSQL_DATABASE"],
                ssl=os.getenv("MYSQL_SSL", "0").lower() in ("1", "true", "yes", "on"),
            )
        except KeyError as e:
            missing_var = e.args[0]
            raise ValueError(f"Missing {missing_var} environment variable") from None

    @classmethod
    def from_pg_env(cls) -> Route:
        try:
            return Route(
                database_type=DatabaseType.POSTGRESQL,
                host=os.environ["PGHOST"],
                port=int(os.environ["PGPORT"]),
                user=os.environ["PGUSER"],
                password=os.environ.get("PGPASSWORD"),  # May use `PGPASSFILE` instead.
                database=os.environ["PGDATABASE"],
                ssl=os.getenv("PGREQUIRESSL", "0").lower() in ("1", "true", "yes", "on"),
            )
        except KeyError as e:
            missing_var = e.args[0]
            raise ValueError(f"Missing {missing_var} environment variable") from None

    @classmethod
    def from_env(cls, *, database_type: Optional[DatabaseType] = None) -> Route:
        database_types = [database_type] if database_type else list(DatabaseType)
        e: Optional[Exception] = None
        for dt in database_types:
            try:
                factory = dt.switch(mysql=cls.from_mysql_env, postgresql=cls.from_pg_env)
                return factory()
            except ValueError as _e:
                e = e or _e
                continue
        assert e
        raise e

    def __init__(
        self,
        *,
        database_type: DatabaseType,
        host: str,
        port: int,
        user: str,
        password: Optional[str] = None,
        database: Optional[str] = None,
        iam_auth: bool = False,
        expires_at: Optional[int] = None,
        ssl: bool = False,
        bastion_host: Union[Optional[str], DefaultType] = Default,
        boto3_session: Optional[Boto3Session] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self._password = password
        self.database_type = database_type
        self.database = database
        self.iam_auth = iam_auth
        self.expires_at = expires_at
        self.ssl = ssl

        # For `bastion_host`, `None` is a meaningful value distinct from the default:
        self.bastion_host: Optional[str]
        if bastion_host is Default:
            self.bastion_host = os.environ.get("ALTITUDE_BASTION_HOST")
        else:
            self.bastion_host = cast(Optional[str], bastion_host)  # Oh, mypy ...

        self.boto3_session = boto3_session or Boto3SessionGenerator().generate_default_session()

    @contextmanager
    def physical_route(self) -> Iterator[Route]:
        """
        Context manager that produces a physical route in case any kind of virtual
        routing, such as an SSH tunnel, is required. Physical connections should be
        established during the context. The ability to physically connect is not
        guaranteed outside this context.

        If `bastion_host` constructor argument or `ALTITUDE_BASTION_HOST` environment
        variable are defined, sets up an SSH tunnel using `/usr/bin/ssh` and forwards
        database connections through it.

        For example:

            with route.physical_route() as physical_route:
                connect(host=physical_route.host, port=physical_route.port)
        """
        if self.bastion_host:
            connection_id = str(self).encode()
            host, port = "127.0.0.1", (54000 + binascii.crc32(connection_id) % 1000)
            port_forward = PortForward(local_port=port, host=self.host, port=self.port)
            ssh_tunnel = SSHTunnel(bastion_host=self.bastion_host, port_forwards=[port_forward])
            ssh_tunnel.wait()
            yield self.replace(host=host, port=port)
        else:
            yield self

    def replace(self, **kwargs: Any) -> Route:
        attrs = self.__dict__.copy()
        attrs["password"] = self._password
        del attrs["_password"]
        attrs.update(kwargs)
        return self.__class__(**attrs)

    def refresh_iam_token(self) -> None:
        if not self.iam_auth:
            return
        token = RdsConnect(boto3_session=self.boto3_session).generate_db_auth_token(
            host=self.host, port=self.port, user=self.user
        )
        query_string = urllib.parse.parse_qs(urllib.parse.urlparse(token).query)
        expires_at = int(time.monotonic()) + int(query_string["X-Amz-Expires"][0])
        self._password, self.expires_at = token, expires_at

    def __str__(self) -> str:
        return self.anonymous_sql_alchemy_connection_url

    @property
    def global_db_route(self) -> Route:
        return self.replace(database=self.database_type.switch(mysql=None, postgresql="postgres"))

    @property
    def expired(self) -> bool:
        return self.expires_at is not None and time.monotonic() >= self.expires_at

    @property
    def password(self) -> Optional[str]:
        if self.expired:
            self.refresh_iam_token()
        return self._password

    @property
    def driver_module(self) -> ModuleType:
        return self.database_type.switch(mysql=pymysql, postgresql=psycopg2)

    @property
    def sql_alchemy_dialect(self) -> str:
        return self.database_type.switch(mysql="mysql+pymysql", postgresql="postgresql")

    @property
    def anonymous_sql_alchemy_connection_url(self) -> str:
        database = self.database or ""
        url = f"{self.sql_alchemy_dialect}://{self.host}:{self.port}/{database}"
        params = {}
        if self.iam_auth:
            params["iam_auth"] = "true"
        if params:
            params_str = "&".join(f"{key}={value}" for key, value in params.items())
            url = f"{url}?{params_str}"
        return url

    @property
    def sql_alchemy_connection_url(self) -> str:
        database = self.database or ""
        if self.password:
            authority = f"{self.user}:{urllib.parse.quote_plus(self.password)}"
        else:
            authority = self.user
        url = f"{self.sql_alchemy_dialect}://{authority}@{self.host}:{self.port}/{database}"
        params = {}
        if self.iam_auth:
            params["iam_auth"] = "true"
        if params:
            params_str = "&".join(f"{key}={value}" for key, value in params.items())
            url = f"{url}?{params_str}"
        return url

    @property
    def connect_args(self) -> dict:
        args: Dict[str, Any] = {}
        if self.ssl:
            ssl_verify_peer = not (
                # Do not verify SSL certificate if SSH tunnel or local connection.
                self.bastion_host
                or self.host == "127.0.0.1"
            )
            if self.database_type == DatabaseType.MYSQL:
                args["ssl"] = {
                    "capath": str(CA_CERTS_PATH),
                    "check_hostname": ssl_verify_peer,
                }
            elif self.database_type == DatabaseType.POSTGRESQL:
                args["sslrootcert"] = str(CA_CERT_FILENAME)
                args["sslmode"] = "verify-full" if ssl_verify_peer else "require"
        return args
