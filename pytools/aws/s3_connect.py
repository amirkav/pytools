import json
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, TextIO, Tuple, Union

from botocore.exceptions import ClientError

from pytools.common import list_utils
from pytools.aws.boto3_connect import Boto3Connect
from pytools.aws.boto3_session_generator import Boto3Session
from pytools.common.deprecated import deprecated
from pytools.common.file_utils import ManagedTempFile
from pytools.common.logger import Logger

RawAWSResponse = Any


#######################################
class S3Connect(Boto3Connect):
    """
    TROUBLESHOOTING: To allow Lambda to download objects from S3,
    you need to add permissions in the following places:
    (a) Add s3:GetObject permission to Lambda's execution role, and set the resource to the S3
    bucket or set s3:*
    (b) Add s3:GetObject permission on the S3 bucket policy, and set the Principal to the Lambda
    execution role.
    (c) If your S3 objects are encrypted using a user-specified encryption key,
          you need to allow the IAM Role to use the encryption key.
          AWS KMS > Encryption Keys > Key Users > Add > Add the IAM Role


    ## DEBUGGING: Assume IAM Role BBS-Dev-Lambda-VPC-Execution-Role
    Note: when executing via Lambda, it uses the Lambda Execution IAM Role:
    arn:aws:sts::474602133305:assumed-role/BBS-Dev-Lambda-VPC-Execution-Role/admin_directory_lambda
    So, for debugging (to see how we can allow Lambda to read from the S3 bucket),
    we first assume Lambda's Execution Role here, and then try to download the file from S3.
    After we are done with debugging, we can remove/comment out this section.
    https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example.html

    import boto3
    from botocore.client import Config
    session = boto3.Session()
    sts_client = session.client("sts", "us-west-2", config=Config(signature_version='s3v4'))
    response = sts_client.assume_role(
        RoleArn="arn:aws:iam::474602133305:role/BBS-Dev-Lambda-VPC-Execution-Role",
        # RoleArn="arn:aws:iam::474602133305:role/BBS-Dev-Developer-Role",
        RoleSessionName="boto3_test",
        DurationSeconds=3600)
    access_key_id = response.get('Credentials').get('AccessKeyId')
    secret_access_key = response.get('Credentials').get('SecretAccessKey')
    session_token = response.get('Credentials').get('SessionToken')

    add credentials from assume_role()
    Note: for assume_role() to work, you need to provide permissions in two places:
    (a) add permission to the IAM User to assume the role. IAM > Users > AssumeRole rules > Edit
    (b) add Trust Relationship to the IAM Role to be assumed by the user.
        IAM > Roles > Trust Relationships > Add the IAM User
    http://boto3.readthedocs.io/en/latest/reference/services/sts.html#STS.Client.assume_role
    https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html
    https://docs.aws.amazon.com/cli/latest/userguide/cli-roles.html
    https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
    http://boto3.readthedocs.io/en/latest/reference/core/session.html#boto3.session.Session.client

    ```python
    s3_client = session.client("s3", "us-west-2",
                               aws_access_key_id=access_key_id,
                               aws_secret_access_key=secret_access_key,
                               aws_session_token=session_token,
                               config=Config(signature_version='s3v4'))
    print("checkpoint after creating client with STS temp tokens: {}".format(response))

    buckets = s3_client.list_buckets()
    print("checkpoint after listing buckets: {}".format(buckets))

    bucket_name = "bbs-seneca-conf-pub"
    bucket_name = "bbs-seneca-conf"
    objects = s3_client.list_objects(Bucket=bucket_name)
    print("checkpoint after listing objects: {}".format(objects))

    key = "awslogs.conf"
    key = "DriveSecurity-6355344cc1e0.json"
    s3_client.download_file(Bucket=bucket_name,
                            Key=key,
                            Filename="{}/{}.bkp".format(os.environ['CONF_DIR'], key))
    print("checkpoint after downloading objects: {}".format("config.json"))
    ```
    """

    @property
    def service(self) -> str:
        """
        Boto3 service name.
        """
        return "s3"

    @property
    def max_delete_objects(self) -> int:
        """
        Max delete objects count at a time.
        """
        return 1000

    def list_s3_keys(self, bucket_name: str, prefix: str = "") -> List[str]:
        """
        List the s3 keys from a S3 bucket and an optional prefix
        https://stackoverflow.com/questions/3337912/quick-way-to-list-all-files-in-amazon-s3-bucket

        Arguments:
            bucket_name -- AWS S3 bucket name
            prefix -- s3 key prefix

        Returns:
            A list of matching S3 keys.
        """
        s3_response = self.list_objects(bucket_name, Prefix=prefix)

        if s3_response.get("Contents") is None:
            self.logger.debug(f"No objects with prefix '{prefix}' exist in bucket '{bucket_name}'.")
            return []

        return [con.get("Key") for con in s3_response.get("Contents")]

    def yield_s3_keys(
        self, bucket_name: str, prefix: Optional[str] = None
    ) -> Iterator[Optional[str]]:
        """
        Iterate over s3 keys from a S3 bucket and an optional prefix

        Arguments:
            bucket_name -- AWS S3 bucket name
            prefix -- s3 key prefix

        Yields:
            S3 keys, one key at a time
        """

        paginate_options = {
            "Bucket": bucket_name,
        }
        if prefix:
            paginate_options["Prefix"] = prefix
        for response in self.client.get_paginator("list_objects_v2").paginate(**paginate_options):
            for contents in response["Contents"]:
                yield contents["Key"]

    def list_all_objects(self, bucket: str) -> List[Dict[str, Any]]:
        """
        List all AWS S3 bucket objects for a given bucket

        Arguments:
            bucket -- AWS S3 bucket name

        Returns:
            List of AWS S3 bucket objects
        """
        response = self.list_objects(bucket)
        contents = response.get("Contents", [])
        marker = contents[-1]["Key"]
        while True:
            response = self.list_objects(bucket, Marker=marker)
            if not response.get("Contents"):
                break
            contents.extend(response["Contents"])
            marker = contents[-1]["Key"]
        return contents

    def list_objects(self, bucket: str, **kwargs: Any) -> RawAWSResponse:
        """
        List AWS S3 bucket objects

        [boto3 - list_objects](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
        )

        Arguments:
            bucket -- AWS S3 bucket name

        Returns:
            AWS response for S3 list objects
        """
        return self.client.list_objects(Bucket=bucket, **kwargs)

    def list_all_objects_v2(
        self, bucket: str, prefix: Optional[str] = None, delimiter: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        List all AWS S3 bucket objects for a given bucket

        Arguments:
            bucket -- AWS S3 bucket name
            prefix -- Optional prefix for AWS S3 keys

        Returns:
            List of AWS S3 bucket objects
        """
        kwargs = {"Bucket": bucket}
        if prefix:
            kwargs["Prefix"] = prefix
        if delimiter:
            kwargs["Delimiter"] = delimiter
        response = self.client.list_objects_v2(**kwargs)
        contents = response.get("Contents", [])
        while response["IsTruncated"]:
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
            response = self.client.list_objects_v2(**kwargs)
            contents.extend(response.get("Contents", []))
        return contents

    def list_objects_v2(self, bucket: str, **kwargs: Any) -> RawAWSResponse:
        """
        Version 2 for listing AWS S3 objects. This is recommended for future use

        [boto3 - list_objects_v2](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects_v2
        )

        Arguments:
            bucket -- AWS S3 bucket name
            kwargs -- Extra keywprds arguments for `list_objects_v2`

        Returns:
            Raw AWS response for `list_objects_v2`
        """
        return self.client.list_objects_v2(Bucket=bucket, **kwargs)

    @deprecated(reason="Use S3Connect.download_file instead")
    def download_file_from_s3(
        self, bucket_name: str, key: str, filename: str
    ) -> Optional[RawAWSResponse]:
        """
        Download a file for an AWS S3 bucket (deprecated)

        Arguments:
            bucket_name -- AWS S3 bucket name
            key -- AWS S3 key
            filename -- Destination filename

        Returns:
            AWS response for download_file
        """
        self.logger.debug(
            f"Downloading S3 object {S3Connect.get_s3_path(bucket_name, key)} to {filename} on disk"
        )
        try:
            return self.download_file(bucket_name, key, filename)
        except ClientError as e:
            self.logger.exception(e, level=Logger.WARNING)
            if e.response["Error"]["Code"] == "404":
                self.logger.warning(f"S3 object s3://{bucket_name}/{key} does not exist.")
                return None
            if e.response["Error"]["Code"] == "403":
                self.logger.warning(
                    f"Access to S3 object s3://{bucket_name}/{key} denied; "
                    f"make sure your IAM has access to this bucket."
                )
                return None
            raise

    def download_file_to_path(
        self,
        bucket: str,
        key: str,
        dest_path: Optional[Path] = None,
        exists_ok: bool = False,
    ) -> Path:
        """
        Downloads an existing S3 file object to a given path.

        Args:
            bucket (str): AWS S3 bucket name
            key (str): AWS S3 key of the file
            dest_path (Path): Optional destination path. If not provided,
                a temporary path will be used.
            exists_ok (bool, optional): True if okay path already exists.
                Otherwise, will raise a FileExistsError. (Default False)

        Returns:
            Path object for downloaded s3 file object
        """
        if dest_path is None:
            with tempfile.NamedTemporaryFile() as temp_file:
                dest_path = Path(temp_file.name)
        dest_path.touch(exist_ok=exists_ok)
        self.client.download_file(
            Bucket=bucket,
            Key=key,
            Filename=str(dest_path),
        )
        return dest_path

    def download_file(self, bucket: str, key: str, filename: str) -> Optional[RawAWSResponse]:
        """
        Download a file from an AWS S3 bucket

        [boto3 - download_file](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
        )

        Arguments:
            bucket -- AWS S3 bucket name
            key -- AWS S3 key
            filename -- Destination filename

        Returns:
            AWS response for download_file
        """
        try:
            return self.client.download_file(Bucket=bucket, Key=key, Filename=filename)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.logger.warning(f"S3 object s3://{bucket}/{key} does not exist.")
                return None
            if e.response["Error"]["Code"] == "403":
                self.logger.warning(
                    f"Access to S3 object s3://{bucket}/{key} denied; "
                    f"make sure your IAM has access to this bucket."
                )
                return None

            self.logger.exception(e)
            raise

    def download_file_v2(self, bucket: str, key: str, filename: str) -> RawAWSResponse:
        """
        Download a file from an AWS S3 bucket. Is not graceful about errors, just raises them.

        [boto3 - download_file](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
        )

        Arguments:
            bucket -- AWS S3 bucket name
            key -- AWS S3 key
            filename -- Destination filename

        Returns:
            AWS response for download_file
        """
        return self.client.download_file(Bucket=bucket, Key=key, Filename=filename)

    def read_data_from_s3(self, bucket_name: str, key: str) -> Optional[bytes]:
        """
        Read data from an AWS S3 object

        Arguments:
            bucket_name -- AWS S3 bucket name
            key -- AWS S3 key

        Returns:
            S3 object contents as bytes.
        """
        self.logger.debug(f"Reading data from {S3Connect.get_s3_path(bucket_name, key)}")

        return self.get_object(bucket_name, key)

    def get_object(self, bucket: str, key: str, **kwargs: Any) -> Optional[bytes]:
        """
        Get an AWS S3 object

        [boto3 - get_object](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        )

        Arguments:
            bucket -- AWS S3 bucket
            key -- AWS S# key

        Returns:
            Raw AWS response for `get_object`
        """
        try:
            s3_response = self.client.get_object(Bucket=bucket, Key=key, **kwargs)
            return s3_response["Body"].read()

        except ClientError as e:
            self.logger.exception(e, level=Logger.WARNING)
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.warning(f"S3 object s3://{bucket}/{key} does not exist.")
                return None

            if e.response["Error"]["Code"] == "403":
                self.logger.warning(
                    f"Access to S3 object s3://{bucket}/{key} denied; "
                    f"make sure your IAM has access to this bucket."
                )
                return None

            self.logger.exception(e)
            raise

    def get_object_v2(self, bucket: str, key: str, **kwargs: Any) -> bytes:
        """
        Get an AWS S3 object. Is not graceful about errors, just raises them.

        [boto3 - get_object](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
        )

        Arguments:
            bucket -- AWS S3 bucket
            key -- AWS S# key

        Returns:
            Raw AWS response for `get_object`
        """
        s3_response = self.client.get_object(Bucket=bucket, Key=key, **kwargs)
        return s3_response["Body"].read()

    def put_object(self, bucket: str, key: str, **kwargs: Any) -> RawAWSResponse:
        """
        Put an object in an AWS S3 bucket

        [boto3 - put_object](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_object
        )

        Arguments:
            bucket -- AWS S3 bucket name
            key -- AWS S3 key

        Returns:
            AWS response for `put_object`
        """
        return self.client.put_object(Bucket=bucket, Key=key, **kwargs)

    def upload_data_to_s3(
        self, data: str, bucket_name: str, key: str, kms_key: Optional[str] = None
    ) -> RawAWSResponse:
        """
        https://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.put_object
        About encryption kms key: boto3 will automatically add encryption key data for encrypted
        buckets.
        So, if you are encrypting the object with the same key as its parent bucket,
        there is no need to provide encryption key.
        But if you want to encrypt an object with a key different than its parent bucket,
        then you can provide encryption key to this function.

        Arguments:
            data -- a stream of bytes or a file-like object
            bucket_name -- S3 bucket name
            key -- S3 object key
            kms_key -- arn of the aws kms key to encrypt the data at rest on s3

        Returns:
            Raw AWS response for `put_object`
        """
        self.logger.debug(f"Storing data in {S3Connect.get_s3_path(bucket_name, key)}")

        if kms_key:
            s3_response = self.put_object(
                bucket_name,
                key,
                Body=data,
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=kms_key,
            )
        else:
            s3_response = self.put_object(bucket_name, key, Body=data)

        return s3_response

    @deprecated(reason="Use S3Connect.yield_data_from_s3_keys instead")
    def yield_data_from_s3(
        self, s3_buckets: Union[str, List[str]], s3_keys: List[str]
    ) -> Iterator[Optional[bytes]]:
        """
        This generator is recommended for smaller S3 files that can be easily loaded into memory.
        If you are reading larger files, it is recommended to use download_file_from_s3() and
        yield_records_from_file() methods to avoid loading large files on memory.

        Arguments:
            s3_buckets -- the s3 bucket
            s3_keys -- the s3 keys
            kms_key_arn -- the encryption key for the bucket

        Yields:
            Records in the s3 objects, one file at a time
        """
        if isinstance(s3_buckets, str):
            s3_buckets = [s3_buckets]
        if len(s3_buckets) == 1:
            s3_buckets *= len(s3_keys)

        for s3_bucket, s3_key in zip(s3_buckets, s3_keys):
            data_str = self.read_data_from_s3(bucket_name=s3_bucket, key=s3_key)
            yield data_str

    def put_bucket_notification_configuration(
        self, bucket: str, notification_config: Dict[str, Any]
    ) -> RawAWSResponse:
        """
        Put an AWS S3 bucket notification

        [boto3 - put_bucket_notification_configuration](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_notification_configuration
        )

        Arguments:
            bucket -- AWS S3 bucket
            notification_config -- AWS S3 bucket notification configuration

        Returns:
            AWS response put_bucket_notification_configuration
        """

        return self.client.put_bucket_notification_configuration(
            Bucket=bucket, NotificationConfiguration=notification_config
        )

    def get_bucket_notification_configuration(self, bucket: str) -> RawAWSResponse:
        """
        Get an AWS S3 bucket notification

        [boto3 - put_bucket_notification_configuration](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_bucket_notification_configuration
        )

        Arguments:
            bucket -- AWS S3 bucket

        Returns:
            AWS response get_bucket_notification_configuration
        """
        return self.client.get_bucket_notification_configuration(Bucket=bucket)

    def create_bucket(self, bucket: str, **kwargs: Any) -> RawAWSResponse:
        """
        Create an AWS S3 bucket

        [boto3 - create_bucket](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
        )

        Arguments:
            bucket -- AWS S3 bucket name

        Returns:
            AWS response for create_bucket
        """
        try:
            return self.client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": self.aws_region},
                **kwargs,
            )
        except self.client.exceptions.BucketAlreadyOwnedByYou:
            self.logger.warning(f"Bucket {bucket} already exists, skipping create")
            return None

    def delete_bucket(self, bucket: str) -> RawAWSResponse:
        """
        Delete an AWS S3 bucket

        [boto3 - delete_bucket](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_bucket
        )

        Arguments:
            bucket -- AWS S3 bucket

        Returns:
            AWS response for delete_bucket
        """
        return self.client.delete_bucket(Bucket=bucket)

    def delete_all_objects(self, bucket: str, quiet: bool = False) -> int:
        """
        Delete all objects from an AWS S3 bucket

        Arguments:
            bucket -- AWS S3 bucket name
            quiet -- True for quiet logs (default: {False})

        Returns:
            Number of objects deleted
        """
        bucket_contents = self.list_all_objects_v2(bucket)
        bucket_keys = list(map(lambda o: {"Key": o["Key"]}, bucket_contents))
        object_delete_partitions = list(list_utils.chunkify(bucket_keys, self.max_delete_objects))
        num_objects_deleted = 0
        self.logger.debug(f"Attempting to delete {len(bucket_keys)} objects from s3://{bucket}")
        for object_delete_partition in object_delete_partitions:
            self.logger.debug(f"Delete progress {num_objects_deleted}/{len(bucket_keys)}")
            self.delete_objects(bucket=bucket, objects=object_delete_partition, quiet=quiet)
            num_objects_deleted += len(object_delete_partition)
        self.logger.debug(f"Deleted {num_objects_deleted} from s3://{bucket}")
        return num_objects_deleted

    def delete_object(self, bucket: str, key: str, **kwargs: Any) -> RawAWSResponse:
        """
        Delete an AWS S3 object from a given bucket

        [boto3 - delete_object](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object
        )

        Arguments:
            bucket -- AWS S3 bucket name
            key -- AWS S3 object key

        Returns:
            AWS response for delete_object
        """
        return self.client.delete_object(Bucket=bucket, Key=key, **kwargs)

    def delete_objects(
        self,
        bucket: str,
        objects: List[Dict[str, str]],
        quiet: bool = False,
        **kwargs: Any,
    ) -> RawAWSResponse:
        """
        Delete AWS S3 objects from a given bucket

        [boto3 - delete_objects](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
        )

        Arguments:
            bucket -- AWS S3 bucket name
            objects -- AWS S3 object keys and optional version
                for example:
                - `basic = [{"Key": "folder/1234565}]`
                - `version = [{"Key": "folder/1234565, "VersionId": "123"}])`
            quiet -- True for quiet logs (default: {False})

        Returns:
            AWS response for delete_objects
        """
        return self.client.delete_objects(
            Bucket=bucket, Delete={"Objects": objects, "Quiet": quiet}, **kwargs
        )

    def put_bucket_lifecycle_configuration(
        self, bucket: str, lifecycle_config: Dict[str, List[Any]]
    ) -> RawAWSResponse:
        """
        Put an AWS S3 lifecycle configuration

        [boto3 - put_bucket_lifecycle_configuration](
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_lifecycle_configuration)

        Arguments:
            bucket -- AWS S3 bucket name
            lifecycle_config -- AWS S3 life cycle configuration

        Returns:
            AWS response for put_bucket_lifecycle_configuration
        """
        return self.client.put_bucket_lifecycle_configuration(
            Bucket=bucket, LifecycleConfiguration=lifecycle_config
        )

    def put_bucket_encryption(
        self, bucket: str, sse_config: Dict[str, List[Any]], **kwargs: Any
    ) -> RawAWSResponse:
        """
        Put encryption on an AWS S3 bucket

        [boto3 - put_bucket_encryption](
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_encryption)

        Arguments:
            bucket -- AWS S3 bucket name
            sse_config - AWS S3 server side encryption configuration

        Returns:
            AWS response for put_bucket_encryption
        """
        return self.client.put_bucket_encryption(
            Bucket=bucket, ServerSideEncryptionConfiguration=sse_config, **kwargs
        )

    def put_bucket_policy(self, bucket: str, policy: str, **kwargs: Any) -> RawAWSResponse:
        """
        Put an AWS S3 bucket policy on a given bucket

        [boto3 - put_bucket_policy](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_policy
        )

        Arguments:
            bucket -- AWS S3 bucket name
            policy -- AWS S3 bucket policy

        Returns:
            AWS response for put_bucket_policy
        """
        return self.client.put_bucket_policy(Bucket=bucket, Policy=policy, **kwargs)

    def put_bucket_tagging(self, bucket: str, tagging: Dict[str, List[Any]]) -> RawAWSResponse:
        """
        Put AWS S3 bucket tagging on a given bucket

        [boto3 - put_bucket_tagging](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_tagging
        )

        Arguments:
            bucket -- AWS S3 bucket name
            tagging -- AWS S3 bucket tag set

        Returns:
            AWS response for put_bucket_tagging
        """
        return self.client.put_bucket_tagging(Bucket=bucket, Tagging=tagging)

    def put_bucket_accelerate_configuration(self, bucket: str, enabled: bool) -> RawAWSResponse:
        """
        Toggle an AWS S3 Buckets transfer acceleration configuration

        [boto3 - put_bucket_accelerate_configuration](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.put_bucket_accelerate_configuration
        )

        Arguments:
            bucket -- AWS S3 bucket name
            enabled -- True to enable AWS S3 transfer acceleration

        Returns:
            Raw AWS response for put_bucket_accelerate_configuration
        """
        return self.client.put_bucket_accelerate_configuration(
            Bucket=bucket,
            AccelerateConfiguration={"Status": "Enabled" if enabled else "Suspended"},
        )

    def yield_data_from_s3_keys(
        self, s3_buckets: Union[str, List[str]], s3_keys: List[str]
    ) -> Iterator[Tuple[str, str, Optional[bytes]]]:
        """
        This generator is recommended for smaller S3 files that can be easily loaded into
        memory.
        If you are reading larger files, it is recommended to use download_file_from_s3() and
        yield_records_from_file() methods to avoid loading large files on memory.

        Arguments:
            s3_buckets -- the s3 bucket
            s3_keys -- the s3 keys

        Returns:
            A generator that yields records in the s3 objects, one file at a time
        """
        if isinstance(s3_buckets, str):
            s3_buckets = [s3_buckets]
        if len(s3_buckets) == 1:
            s3_buckets *= len(s3_keys)

        for s3_bucket, s3_key in zip(s3_buckets, s3_keys):
            data_str = self.read_data_from_s3(bucket_name=s3_bucket, key=s3_key)
            yield s3_bucket, s3_key, data_str

    def copy_all(
        self,
        source_bucket: str,
        destination_bucket: str,
        acl: str = "bucket-owner-full-control",
        max_concurrency: int = 50,
        source_path: Path = Path(""),
        destination_path: Path = Path(""),
        skip_existing: bool = True,
    ) -> int:
        """Copies all objects from a S3 bucket source to a S3 bucket destination

        Arguments:
            source_bucket -- AWS S3 bucket name for source
            destination_bucket -- AWS S3 bucket name for destination
            acl -- Destination objects ACL
            max_concurrency -- Max threads
            source_path -- Path in source bucket
            destination_path -- Path in destination bucket
            skip_existing -- Skip objects with matching ETags

        Returns:
            Number of keys copied
        """
        self.logger.debug(f"Copying all objects from {source_bucket} to {destination_bucket}")
        source_objects = self.list_all_objects_v2(
            source_bucket, prefix=self.normalize_s3_path(Path(source_path))
        )
        source_etag_map: Dict[str, str] = {i["Key"]: i["ETag"] for i in source_objects}
        existing_keys_set: Set[str] = set()
        if skip_existing:
            dest_objects = self.list_all_objects_v2(
                destination_bucket, prefix=self.normalize_s3_path(Path(destination_path))
            )
            dest_etag_map: Dict[str, str] = {
                i["Key"]: i["ETag"] for i in dest_objects if i["Key"] in source_etag_map
            }
            existing_keys = [k for k, v in source_etag_map.items() if dest_etag_map.get(k) == v]
            existing_keys_set = set(existing_keys)
            if existing_keys:
                self.logger.debug(f"{len(existing_keys)} already exist, skipping")

        source_keys = [k for k in source_etag_map if k not in existing_keys_set]
        dest_keys = [self.normalize_s3_path(Path(destination_path) / i) for i in source_keys]

        def _copy_object(source_key: str, dest_key: str) -> Tuple[str, str]:
            self.client.copy_object(
                ACL=acl,
                Bucket=destination_bucket,
                Key=dest_key,
                CopySource={"Key": source_key, "Bucket": source_bucket},
            )
            return source_key, dest_key

        with ThreadPoolExecutor(max_workers=max_concurrency) as executor:
            for source_key, dest_key in executor.map(
                _copy_object, source_keys, dest_keys, chunksize=max_concurrency
            ):
                self.logger.debug(
                    f"Copied {self.get_s3_path(source_bucket, source_key)}"
                    f" to {self.get_s3_path(destination_bucket, dest_key)}"
                )

        self.logger.debug(
            f"{len(source_keys)} objects from {source_bucket} were copied to {destination_bucket}"
        )
        return len(source_keys)

    def copy(
        self,
        source_bucket: str,
        source_key: str,
        destination_bucket: str,
        destination_key: Optional[str] = None,
        source_client: Optional[Boto3Session] = None,
        **kwargs: Any,
    ) -> RawAWSResponse:
        """Copies an object key from a source bucket to a destination bucket

        [boto3 - copy](
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.copy
        )

        Arguments:
            source_bucket -- AWS S3 bucket name for source
            source_key -- AWS S3 object key from source bucket
            destination_bucket -- AWS S3 bucket name for destination

        Keyword Arguments:
            destination_key {Optional} -- AWS S3 object key for destination (default: {source key})
            source_client {Optional} -- AWS boto3 client to use for source s3 connection (
                default: {default boto3 session})

        Returns:
            Raw AWS response for copy
        """
        return self.client.copy(
            CopySource={"Bucket": source_bucket, "Key": source_key},
            Bucket=destination_bucket,
            Key=(destination_key or source_key),
            SourceClient=(source_client or self.client),
            **kwargs,
        )

    def upload_file_to_s3(
        self, bucket_name: str, key: str, local_file: str, kms_key_arn: str
    ) -> Dict[str, int]:
        """
        Upload local file to S3.

        Arguments:
             bucket_name -- s3 bucket name
             key -- s3 key prefix
             local_file -- path of local file to be uploaded to s3
             kms_key_arn -- key to encrypt file with, pass an empty string to disable encryption

        Returns:
            A dict object containing an indicator for S3 store success
        """
        self.logger.debug(f"Uploading {local_file} to {S3Connect.get_s3_path(bucket_name, key)}")
        extra_args = {}
        if kms_key_arn:
            extra_args.update({"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": kms_key_arn})

        self.client.upload_file(
            Bucket=bucket_name,
            Key=key,
            Filename=local_file,
            ExtraArgs=extra_args,
        )
        return {"store_success": 1}

    @contextmanager
    def write_to_s3_key(self, bucket_name: str, key: str, kms_key_arn: str) -> Iterator[TextIO]:
        """
        A managed generator to
            - write data to a local temp file
            - upload it to a s3 location
            - delete the local temp file

        Arguments:
            bucket_name -- s3 bucket name
            key -- s3 key prefix
            kms_key_arn -- key to encrypt file with

        Yields:
            An opened temporary file stream.
        """
        temp_file = ManagedTempFile().get_temp_file()
        stream = None
        try:
            stream = open(temp_file, mode="w", encoding="UTF-8")
            yield stream
        finally:
            if stream:
                stream.close()
            if ManagedTempFile.is_non_empty_file(temp_file):
                self.upload_file_to_s3(bucket_name, key, temp_file, kms_key_arn)
            ManagedTempFile.delete_file(temp_file)

    @staticmethod
    def normalize_s3_path(path: Path) -> str:
        """
        Get normalized key path in bucket.

        If path is absolute - converts it to relative.

        Arguments:
            path -- Path-like object.
        """
        path_obj = Path(path)
        if not path_obj.is_absolute():
            return path_obj.as_posix().rstrip(".")

        return path_obj.as_posix().lstrip("/").rstrip(".")

    @staticmethod
    def get_s3_path(bucket_name: str, key: str) -> str:
        return f"s3://{bucket_name}/{key}"

    def grant_user_object_acl(self, bucket: str, key: str, canonical_id: str) -> RawAWSResponse:
        """
        Grants a AWS user access to a given s3 Object

        Arguments:
            bucket -- AWS S3 Bucket Name
            key -- AWS S3 Key to update
            canonical_id -- AWS User canonical ID

        Returns:
            AWS Response for put_object_acl
        """
        return self.client.put_object_acl(
            Bucket=bucket, Key=key, GrantFullControl=f"id={canonical_id}"
        )

    def validate_bucket_name(self, bucket: str) -> bool:
        """
        Check if bucket exists.

        Arguments:
            bucket -- Bucket name.

        Returns:
            True if bucket exists.
        """
        try:
            self.client.head_bucket(Bucket=bucket)
            return True
        except ClientError as e:
            self.logger.exception(e, show_traceback=False)
            return False

    def get_bucket_encryption(self, bucket_name: str) -> RawAWSResponse:
        """Gets an existing AWS S3 bucket encryption rule

        [boto3 - get_bucket_encryption](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_bucket_encryption)

        Arguments:
            bucket_name -- AWS S3 bucket name

        Returns:
            AWS response for get_bucket_encryption
        """
        return self.client.get_bucket_encryption(Bucket=bucket_name)


def main() -> None:
    logger = Logger.main(level=Logger.DEBUG)
    s3_connect = S3Connect(aws_region="us-west-2")
    # s3_connect.copy(
    #     source_bucket="thoughtlabs-dev",
    #     source_key="configs/project_configs.yml",
    #     destination_bucket="thoughtlabs-dev",
    #     destination_key="configs/project_configs_bkp2.yml",
    # )

    # Upload
    bucket_name = "thoughtlabs-dev"
    # seneca-key
    # kms_key = "arn:aws:kms:us-west-2:474602133305:key/375c493c-7912-4afd-ad32-8828586f4197"
    # thoughtlabs-key
    # kms_key = "arn:aws:kms:us-west-2:474602133305:key/fafd5c20-5d8e-4059-a754-3fa004ee59e3"
    key = "test-delete.json"
    temp_path = Path(tempfile.gettempdir())
    file_path = temp_path / key

    if not file_path.exists():
        file_path.write_text(json.dumps({"test": "value"}))

    # verify the upload
    key_list_before = s3_connect.list_s3_keys(bucket_name)

    data = json.loads(file_path.read_text())
    logger.json(data, name="data")
    string = json.dumps(data)
    logger.debug(string)
    s3_connect.upload_data_to_s3(string, bucket_name, key)
    # Provide kms_key if you want to use server side encryption
    # s3_connect.upload_data_to_s3(string, bucket_name, key, kms_key)

    # verify the upload
    logger.debug("key_list before upload: {}".format(key_list_before))
    key_list = s3_connect.list_s3_keys(bucket_name)
    logger.debug("key_list after upload: {}".format(key_list))

    # Download file object from S3 to local drive
    download_file_path = temp_path / "test-delete-dl.json"
    s3_connect.download_file_from_s3(bucket_name, key, str(download_file_path))

    # Load data from S3 into memory
    data = s3_connect.read_data_from_s3(bucket_name, key)
    logger.debug(f"type(data): {type(data)}")
    logger.debug(f"data: {data}")

    # List objects
    prefix = "test"
    key_list = s3_connect.list_s3_keys(bucket_name, prefix)
    logger.debug(f"key_list with prefix {prefix}: {key_list}")

    # yield data from s3 keys
    key_list = s3_connect.list_s3_keys(bucket_name, prefix) or []
    logger.debug(f"s3 keys to yield from: \n{key_list}")
    for rec in s3_connect.yield_data_from_s3(bucket_name, key_list):
        if rec:
            logger.debug(str(rec))

    # generator to write to local file and upload it directly to a s3 location
    with s3_connect.write_to_s3_key(bucket_name, key, "") as stream:
        stream.write("testing\n")
        stream.write("testing2\n")

    print(f"thoughtlabs-dev bucket exists: {s3_connect.validate_bucket_name('thoughtlabs-dev')}")
    print(
        "thoughtlabs-dev-bogus bucket exists:"
        f" {s3_connect.validate_bucket_name('thoughtlabs-dev-bogus')}"
    )


if __name__ == "__main__":
    main()
