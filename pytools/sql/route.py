# We may be able to remove this import in future python updates. See:
# https://stackoverflow.com/questions/33533148/how-do-i-type-hint-a-method-with-the-type-of-the-enclosing-class
from __future__ import annotations

import binascii
import os
import time
import urllib.parse
from contextlib import contextmanager
from types import ModuleType
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Union, cast

import psycopg2

from pytools.aws.boto3_session_generator import Boto3Session, Boto3SessionGenerator
from pytools.aws.ssm_connect import SsmConnect
from pytools.aws.rds_connect import RdsConnect
from pytools.resources import CA_CERT_FILENAME

if TYPE_CHECKING:
    from pytools.app.configs import Configs

from .ssh_tunnel import PortForward, SSHTunnel  # pylint: disable=wrong-import-position


class Route:
    LOCAL_HOST_DEFAULT = "127.0.0.1"

    @classmethod
    def from_config(
        cls,
        configs: Configs,
        *,
        bastion_host: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
        ssm_connect: Optional[SsmConnect] = None,
        use_writer: bool = False,
        use_master: bool = False,
    ) -> Route:
        """
        Determine SQL connection info based on project configuration.

        Arguments:
            configs -- `tools.configs.Configs` object that provides AWS region, database endpoints,
                database user names, and the database name.
            boto3_session -- (optional) `boto3.session.Session` object.
            ssm_connect -- (optional) `pytools.ssm_connect.SsmConnect` object that provides database
                passwords and CA SSL certificates.
            use_writer -- (optional) Boolean indicating whether write access is required.
                (default: False)
            use_master -- (optional) Boolean indicating whether master-user access is required.
                If `use_master` is specified, `use_writer` is ignored. (default: False)

        Returns:
            `Route` object describing where to connect and how to authenticate.
        """
        boto3_session = boto3_session or Boto3SessionGenerator().generate_default_session()
        ssm_connect = ssm_connect or SsmConnect(boto3_session=boto3_session)

        host = configs.pg_db_endpoint if use_writer or use_master else configs.pg_db_ro_endpoint
        port = configs.pg_db_port or 5432

        database = configs.db_name
        # Master user cannot use IAM auth:
        iam_auth = False if use_master else configs.db_iam_auth

        # User name:
        if use_master:
            user = configs.pg_master_user
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
            password = ssm_connect.get_parameter_value(pass_parameter, True)

        route = cls(
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
    def from_env(cls) -> Route:
        try:
            return Route(
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

    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str,
        password: Optional[str] = None,
        database: Optional[str] = None,
        iam_auth: bool = False,
        expires_at: Optional[int] = None,
        ssl: bool = False,
        bastion_host: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self._password = password
        self.database = database
        self.iam_auth = iam_auth
        self.expires_at = expires_at
        self.ssl = ssl

        self.bastion_host: Optional[str] = os.environ.get("BASTION_HOST")

        self.boto3_session = boto3_session or Boto3SessionGenerator().generate_default_session()

    @contextmanager
    def physical_route(self) -> Iterator[Route]:
        """
        Context manager that produces a physical route in case any kind of virtual
        routing, such as an SSH tunnel, is required. Physical connections should be
        established during the context. The ability to physically connect is not
        guaranteed outside this context.

        If `bastion_host` constructor argument or `BASTION_HOST` environment
        variable are defined, sets up an SSH tunnel using `/usr/bin/ssh` and forwards
        database connections through it.

        For example:

            with route.physical_route() as physical_route:
                connect(host=physical_route.host, port=physical_route.port)
        """
        if self.bastion_host:
            connection_id = str(self).encode()
            local_host = self.LOCAL_HOST_DEFAULT  # SSH will default to this local address
            local_port = 54000 + binascii.crc32(connection_id) % 1000
            port_forward = PortForward(local_port=local_port, host=self.host, port=self.port)
            ssh_tunnel = SSHTunnel(bastion_host=self.bastion_host, port_forwards=[port_forward])
            ssh_tunnel.wait()
            # At this point, we have established an SSH tunnel to the bastion host, and have forwarded the local_port to its self.port ,
            # so when we connect to local_port, we are actually connecting to self.port on the bastion host.
            # (The bastion host is configured to forward connections to self.port to the database server.)
            yield self.replace(host=local_host, port=local_port)
        else:
            yield self

    def replace(self, **kwargs: Any) -> Route:
        attrs = self.__dict__.copy()
        attrs["password"] = self._password
        del attrs["_password"]
        attrs.update(kwargs)
        return self.__class__(**attrs)

    def refresh_iam_token(self) -> None:
        """
        The IAM token is just a long and strong password that expires shortly.
        When IAM Auth is enabled on the database, the token is used instead of password.
        AWS will automatically rotate the token, so we just need to refresh it when it expires.
        """
        if not self.iam_auth:
            return
        token = RdsConnect(boto3_session=self.boto3_session).generate_db_auth_token(
            host=self.host, port=self.port, user=self.user
        )
        query_string = urllib.parse.parse_qs(urllib.parse.urlparse(token).query)
        expires_at = int(time.monotonic()) + int(query_string["X-Amz-Expires"][0])
        self._password, self.expires_at = token, expires_at

    def __str__(self) -> str:
        return self.sql_alchemy_connection_url

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
        return psycopg2

    @property
    def sql_alchemy_dialect(self) -> str:
        return "postgresql"

    @property
    def sql_alchemy_connection_url(self, redact=False) -> str:
        database = self.database or ""
        if redact:
            authority = "USER_REDACTED:PASSWORD_REDACTED"
        else:
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
                or self.host == self.LOCAL_HOST_DEFAULT
            )
            args["sslrootcert"] = str(CA_CERT_FILENAME)
            args["sslmode"] = "verify-full" if ssl_verify_peer else "require"
        return args
