#!/usr/bin/env python
"""
Generator for boto3 sessions.
"""

import os
from typing import Optional

from boto3.session import Session as Boto3Session
from botocore.exceptions import EndpointConnectionError, ProfileNotFound

from pytools.common.logger import Logger
from pytools.aws.retry_backoff_boto3 import RetryAndBackoffBoto3
from pytools.common.uuid_generator import UuidGenerator

__all__ = ["Boto3Session", "Boto3SessionGenerator"]


class RetryAndBackoffBoto3Session(RetryAndBackoffBoto3):
    """
    Custom retrier that handles `EndpointConnectionError` as well.
    """

    default_exceptions = (EndpointConnectionError,)


class Boto3SessionGenerator:
    """
    Generator for boto3 sessions.

    Arguments:
        default_profile -- Profile name.
        aws_region -- AWS region.
    """

    #######################################
    def __init__(
        self, default_profile: Optional[str] = None, aws_region: Optional[str] = None
    ) -> None:
        self.aws_region = aws_region or os.environ.get("AWS_REGION")
        self.default_profile = default_profile or os.environ.get("AWS_DEFAULT_PROFILE")
        self._logger = Logger(__name__)

    @RetryAndBackoffBoto3Session()
    def generate_default_session(self) -> Boto3Session:
        """
        Generate `Session` for `default_profile`.

        If profile is not found, generates a `Session` with no profile set.

        Returns:
            Generated Session.
        """
        if self.default_profile:
            try:
                return self.generate_profile_session(self.default_profile)
            except ProfileNotFound as e:
                self._logger.debug(
                    f"Cannot generate session for {self.default_profile}"
                    f" profile, generating a default Session instead: {e}"
                )

        return Boto3Session(region_name=self.aws_region)

    @RetryAndBackoffBoto3Session()
    def generate_profile_session(self, profile_name: str) -> Boto3Session:
        """
        Generate `Session` for `profile_name`.
        Arguments:
            profile_name -- Profile name.

        Returns:
            Generated Session.

        Raises:
            botocore.exceptions.ProfileNotFound -- If profile with given tanem was not found.
        """
        self._logger.debug(f"Generating boto3 session for profile {profile_name}")
        return Boto3Session(profile_name=profile_name, region_name=self.aws_region)

    @RetryAndBackoffBoto3Session()
    def generate_session_from_role(self, role_arn: str) -> Boto3Session:
        """
        Generate a Session for `role_arn`.

        Arguments:
            role_arn -- Boto3 role ARN.

        Returns:
            Generated Session.
        """
        default_session = self.generate_default_session()
        sts_client = default_session.client("sts")
        response = sts_client.assume_role(
            RoleArn=role_arn, RoleSessionName=UuidGenerator.generate_uuid_str()
        )
        credentials = response["Credentials"]
        return Boto3Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )


def main() -> None:
    logger = Logger.main(level=Logger.DEBUG)

    session = Boto3SessionGenerator().generate_default_session()
    logger.json(session.profile_name, name="default_profile_name")
    logger.json(session.region_name, name="default_region_name")

    session = Boto3SessionGenerator(
        default_profile="dev", aws_region="us-west-1"
    ).generate_default_session()
    logger.json(session.profile_name, name="custom_profile_name")
    logger.json(session.region_name, name="custom_region_name")

    session = Boto3SessionGenerator(
        default_profile="non_existing", aws_region="us-west-1"
    ).generate_default_session()
    logger.json(session.profile_name, name="non_existing_profile_name")
    logger.json(session.region_name, name="non_existing_region_name")


#######################################
if __name__ == "__main__":
    main()
