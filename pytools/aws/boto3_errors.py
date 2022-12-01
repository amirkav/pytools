"""
Boto3 catches and raises two different types of exceptions:

(a) botocore exceptions
These are statically defined within the botocore package and 
are accessible via botocore.exceptions base class.

(b) AWS service exceptions
These are raised by AWS at runtime, and are not statically defined in botocore.
Instead, AWS service exceptions are caught with the underlying botocore exception, ClientError.
As a result, we cannot reference these errors statically, and must instead catch them at runtime.
This makes it hard to control and finess the way we catch and handle AWS service exceptions. 
Not all service exceptions require the same response; for instance, 
some service exceptions simply require a retry (throttling exceptions),
while others may need a notification.


This module provides fine-grained exceptions with Boto3, beyond the statically defined
`botocore.exceptions.ClientError`.

Boto3 clients raise dynamically defined subclasses of `ClientError` that can be caught by
referencing them as `client.exceptions.XYZ`.
<https://boto3.amazonaws.com/v1/documentation/api/latest/guide/error-handling.html#parsing-error-responses-and-catching-exceptions-from-aws-services>

However the dynamic nature of these exceptions makes it impossible to reference them at compile
time, say, in a function decorator such as `tools.retry_backoff_class.RetryAndCatch`. To facilitate
this we define "proxy" exceptions for the most frequently needed Boto3 exceptions, as well as a
context handler and function decorator that transparently translates dynamic exceptions to these
proxy exceptions.
"""

from contextlib import contextmanager
from typing import Any, Iterator

import botocore.exceptions

# The list of errors that we want to create a proxy class for.
ERRORS = {
    "ThrottlingException",
    "TooManyRequestsException",
    "ValidationError",
}

# A mapping of error codes to proxy classes.
ERROR_CLASS_BY_CODE = {}


# A base class for all proxy exceptions. Simply wraps the botocore ClientError class.
class ServiceError(botocore.exceptions.ClientError):
    """Base class for statically defined boto3 service errors."""


# This informs mypy that this module has dynamically generated attributes and will keep it from
# making assumptions about what attributes exist.
def __getattr__(name: str) -> Any:
    raise AttributeError


# Defines the proxy error classes in the desired namespace.
def define_error_classes(namespace: dict[str, Any]) -> None:
    for class_name in ERRORS:
        # Define constant:
        error_class = type(class_name, (ServiceError,), {})
        namespace[class_name] = ERROR_CLASS_BY_CODE[class_name] = error_class


# Pass `globals()` to define the process error classes in the global namespace.
define_error_classes(globals())


# Use this context manager in your code to catch and
# translate dynamic boto3 exceptions to the proxy error classes.
@contextmanager
def translate_boto3_errors() -> Iterator:
    try:
        yield
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ERROR_CLASS_BY_CODE:
            raise ERROR_CLASS_BY_CODE[error_code](e.response, e.operation_name) from e
        raise
