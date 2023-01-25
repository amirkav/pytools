import warnings
from abc import abstractmethod
from threading import RLock
from typing import Any, Optional

from botocore.client import BaseClient
from botocore.config import Config as Boto3Config
from botocore.exceptions import ClientError

from pytools.aws.boto3_session_generator import Boto3Session, Boto3SessionGenerator
from pytools.common.class_utils import cached_property
from pytools.common.logger import Logger

RawAWSResponse = Any


class Boto3Connect:
    MUTEX = RLock()

    def __init__(
        self,
        *,
        env: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
        aws_region: Optional[str] = None,
        verbose: Optional[int] = None,
        endpoint_url: Optional[str] = None,
        boto3_config: Optional[Boto3Config] = None,
    ):
        """Boto3Connect

        Base class for boto3 connectors

        Note: The Boto3 `standard` retry mode will catch throttling errors and exceptions,
        and will back off and retry them for you.

        [boto3 - client](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/
            core/session.html#boto3.session.Session.client
        )

        Keyword Arguments:
            aws_region -- Optional AWS Region for the boto3 client (default: system default region)
            boto3_session -- Optional boto3 session the client (default: None)
            verbose -- 1 to enable verbose logging (default: 0)
            endpoint_url -- Optional Endpoint URL to use for the client and resource.
                If not provided, botocore will automatically construct the appropriate service URL
            boto3_config -- Optional botocore config to use for clients and resources over default.
        """
        if env:
            warnings.warn(
                "`env` argument is deprecated; It's not required for boto3 calls",
                category=DeprecationWarning,
            )
        self._boto3_session = (
            boto3_session or Boto3SessionGenerator(aws_region=aws_region).generate_default_session()
        )
        self._boto3_config = boto3_config or Boto3Config(
            retries=dict(total_max_attempts=5, mode="standard")
        )
        self.aws_region = str(self.boto3_session.region_name)
        self._endpoint_url = endpoint_url
        if verbose is not None:
            warnings.warn(
                "`verbose` argument is deprecated; increase log level instead",
                category=DeprecationWarning,
            )
        self._logger = Logger.for_object(self)

    @property
    def boto3_session(self) -> Boto3Session:
        return self._boto3_session

    @property
    def endpoint_url(self) -> Optional[str]:
        return self._endpoint_url

    @property
    def boto3_config(self) -> Boto3Config:
        return self._boto3_config

    @property
    @abstractmethod
    def service(self) -> str:
        """
        Override in base connect classes to set the service
        """

    @cached_property
    def client(self) -> BaseClient:
        """Gets the Boto3 client.

        Returns:
            Boto3 client for the service
        """
        # Clients are thread-safe, but the act of creating them is not:
        # <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing>
        with self.MUTEX:
            return self.boto3_session.client(
                self.service,
                config=self.boto3_config,
                region_name=self.aws_region,
                endpoint_url=self.endpoint_url,
            )

    @cached_property
    def resource(self) -> Any:
        # Resources are *not* thread-safe, so don't bother thread-safely creating them.
        # <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/resources.html#multithreading-and-multiprocessing>
        return self.boto3_session.resource(
            self.service,
            config=self.boto3_config,
            endpoint_url=self.endpoint_url,
            region_name=self.aws_region,
        )

    @property
    def logger(self) -> Logger:
        """Get the logger instance.

        Returns:
            Logger instance
        """
        return self._logger

    @staticmethod
    def is_success(resp: RawAWSResponse) -> bool:
        return (resp.get("ResponseMetadata", {})).get("HTTPStatusCode") == 200

    @staticmethod
    def get_error_code(exception: BaseException) -> Optional[str]:
        if isinstance(exception, ClientError):
            return (exception.response.get("Error", {})).get("Code")
        return None
