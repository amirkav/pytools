"""
Client for boto3 STS service
"""
from typing import Any

from pytools.aws.boto3_connect import Boto3Connect
from pytools.aws.boto3_session_generator import Boto3Session
from pytools.common.uuid_generator import UuidGenerator

RawAWSResponse = Any


class StsConnect(Boto3Connect):
    """
    Client for boto3 STS service

    [https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html]
    """

    @property
    def service(self) -> str:
        return "sts"

    def get_account_id(self) -> str:
        """Gets the current AWS account id for the boto3 session

        Returns:
            AWS account id for the current session
        """
        self.logger.debug("Getting AWS account id by calling STS. This is a slow operation.")
        return self.client.get_caller_identity().get("Account")

    def get_assume_role_session(self, role_arn: str) -> Boto3Session:
        """
        Generate a new session for `role_arn`.

        Examples:

            ```python
            role_session = sts_client.get_assume_role_session('my_arn')
            role_s3_connect = S3Connect(boto3_session=role_session)
            ```

        Arguments:
            role_arn -- Assume role ARN.

        Returns:
            A new boto3 session for assumed role.
        """
        session_name = UuidGenerator.generate_uuid_str()
        response = self.client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
        return Boto3Session(
            aws_access_key_id=response["Credentials"]["AccessKeyId"],
            aws_secret_access_key=response["Credentials"]["SecretAccessKey"],
            aws_session_token=response["Credentials"]["SessionToken"],
        )
