#!/usr/bin/env python

from typing import Any

from pytools.aws.boto3_connect import Boto3Connect
from pytools.common.string_utils import StringUtils

RawAWSResponse = Any


class SsmConnectError(Exception):
    pass


class SsmConnect(Boto3Connect):
    @property
    def service(self) -> str:
        return "ssm"

    def put_parameter(
        self, name: str, value: str, param_type: str = "String", **kwargs: Any
    ) -> RawAWSResponse:
        """Puts a AWS SSM parameter

        Arguments:
            name -- AWS SSM parameter name
            value -- AWS SSM parameter type
            param_type -- [description]

        Returns:
            RawAWSResponse -- [description]
        """
        return self._put_parameter(Name=name, Value=value, Type=param_type, **kwargs)

    def _put_parameter(self, **kwargs: Any) -> RawAWSResponse:
        """Wrapper for boto3 put_parameter

        [boto3 - put_parameter](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.put_parameter
        )

        Returns:
            AWS response for put_parameter
        """
        return self.client.put_parameter(**kwargs)

    def get_parameter(self, name: str, **kwargs: Any) -> RawAWSResponse:
        """
        Get an AWS SSM parameter

        [boto3 - get_parameter](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ssm.html#SSM.Client.get_parameter
        )

        Arguments:
            name -- AWS SSM parameter name

        Returns:
            AWS response for get_parameter
        """
        return self.client.get_parameter(Name=name, **kwargs)

    def get_parameter_value(self, name: str, with_decryption: bool = False) -> str:
        """
        Get an AWS SSM parameter value

        Arguments:
            name -- AWS SSM parameter name
            with_decryption -- Whether to decrypt the value (SecureString type)

        Returns:
            Parameter string value.
        """
        response = self.client.get_parameter(Name=name, WithDecryption=with_decryption)
        if not response:
            raise SsmConnectError(f"Parameter {name} not found")

        return response["Parameter"]["Value"]


def main() -> None:
    ssm_connect = SsmConnect()

    # store encrypted data
    ssm_connect.put_parameter("test_ssm_enc", "my_value", "SecureString", Overwrite=True)
    test_ssm_enc = ssm_connect.get_parameter_value("test_ssm_enc")
    ssm_connect.logger.info(f"test_ssm_enc = {test_ssm_enc}")
    test_ssm = ssm_connect.get_parameter_value("test_ssm_enc", True)
    ssm_connect.logger.info(f"test_ssm = {test_ssm}")

    # store plain data
    ssm_connect.put_parameter("test_ssm", "my_value", "String", Overwrite=True)
    test_ssm = ssm_connect.get_parameter_value("test_ssm")
    ssm_connect.logger.info(f"test_ssm = {test_ssm}")


if __name__ == "__main__":
    main()
