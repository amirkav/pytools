"""
Only retries boto3 errors that relate to resource constraints,
such as throttling or throughput exceed.
It raises all other errors.

https://codereview.stackexchange.com/questions/133310/python-decorator-for-retrying-w-exponential-backoff
https://developers.google.com/admin-sdk/directory/v1/limits#backoff

## Retry and backoff logic for boto3

https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html#Programming.Errors.RetryAndBackoff

### 5xx Errors
botocore already implements an exponential backoff,
so when it gives the 5xx errors,
it already did its max tries (max tries can be configured).
So, if you're using boto3, you don't need to retry
original requests that receive server errors (5xx).

See:
https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

If you're not using an AWS SDK such as boto3 ,
you should retry original requests that receive server errors (5xx).

### 4xx Errors

4xx errors depend on the application and the AWS service being used.
However, client errors (4xx, other than a `ThrottlingException`
or a `ProvisionedThroughputExceededException`)
indicate that you need to revise the request itself
to correct the problem before trying again.
So, it's best not to ignore nor retry these errors.

But, for 4xx errors that are related to resource usage and
could be fixed by a retry logic, you need to write your own logic.


## Why boto3 errors need special treatment?
The issue with boto3 client is that it does not implement different exceptions
as individual classes. So, it is impossible to decide whether to
retry an exception based on its class.
Most boto3 exceptions are simply ClientError; but, within ClientError
we have a large number of errors that do not have to be treated the same.
For instance, 'ProvisionedThroughputExceededException' and 'ThrottlingException'
errors are related to provisioned throughput of the Dynamo table,
and should be retried. But these exceptions are not implemented
as their individual classes, so we cannot simple use them in
`except ... ` statement.

See pytools.aws.boto3_errors.py for more details.

See:
https://github.com/boto/boto3/issues/597
"""

from contextlib import contextmanager
from typing import Any, Iterator, List, Tuple, Type

from pytools.aws import boto3_errors
from pytools.common.retry_backoff import RetryAndBackoff, RetryState
from pytools.common.string_utils import StringUtils


class BatchUnprocessedItemsError(Exception):
    """
    Raise if batch operation has unprocessed items.
    Example usage: SQS batch send messages, DynamoDB batch write items.
    """

    def __init__(self, unprocessed_items: List[Any], response: Any) -> None:
        """
        Arguments:
            unprocessed_items -- List of unprocessed items
            response -- Raw AWS response
        """
        self.unprocessed_items = unprocessed_items
        self.response = response
        super().__init__()

    def __str__(self) -> Any:
        return self.response


class RetryAndBackoffBoto3(RetryAndBackoff):
    """
    Add retry and incremental backoff logic to AWS requests

    If function uses batch requests, it must have `previous_responses=None`
    keyword argument to track previous responses list.
    """

    default_exceptions: Tuple[Type[BaseException], ...] = (
        BatchUnprocessedItemsError,
        boto3_errors.ThrottlingException,
        boto3_errors.TooManyRequestsException,
    )

    @classmethod
    @contextmanager
    def translate_errors(cls) -> Iterator:
        """
        Translate dynamically defined AWS service errors to exceptions statically defined in
        `pytools.aws.boto3_errors`.
        """
        with boto3_errors.translate_boto3_errors():
            yield

    def handle_exception(self, exc: BaseException, state: RetryState) -> None:
        if isinstance(exc, BatchUnprocessedItemsError):
            items_count = len(exc.unprocessed_items)
            message = (
                f"{items_count} unprocessed {StringUtils.pluralize(items_count, 'item')} left."
            )
            self._logger.log(msg=message, level=self._log_level)

            previous_responses: List[Any] = []
            if state.method_kwargs and state.method_kwargs.get("previous_responses"):
                previous_responses.extend(state.method_kwargs["previous_responses"])
            previous_responses.append(exc.response)
            if state.method_args is not None:
                state.method_args = (exc.unprocessed_items,) + state.method_args[1:]
            if state.method_kwargs is not None:
                state.method_kwargs = dict(previous_responses=previous_responses)
