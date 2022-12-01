"""
https://codereview.stackexchange.com/questions/133310/python-decorator-for-retrying-w-exponential-backoff
https://developers.google.com/admin-sdk/directory/v1/limits#backoff
"""

import functools
import random
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Type, TypeVar, cast

from pytools.common.logger import Logger
from pytools.common.sentinel import SentinelValue
from pytools.common.string_utils import StringTools

FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


@dataclass
class RetryState:
    """
    Stores the state of the retry loop. This is used for thread-safety.
    It also makes the code cleaner by acting as a container for all the attributes
    that are used in the retry loop.
    """

    method: Callable
    method_parent: Optional[Callable]
    method_args: Tuple
    method_kwargs: Dict[str, Any]

    tries_remaining: int
    exception: Optional[BaseException] = None


#######################################
class RetryAndBackoff:
    """
    A decorator to retry a call on all exceptions except the ones defined in {
    exceptions_to_raise}.
    Works with functions and class methods.

    ```python
    class APIClient:
        @RetryAndBackoff(num_tries=5)
        def get_items(self):
            ...

    @RetryAndBackoff(exceptions_to_raise=(OSError, ))
    def read_files():
        ...
    ```

    Arguments:
        num_tries -- Number of tries before an expected Exception is raised.
        backoff -- Generator that gets original delay and yields delay for retries.
        logger -- Logger instance. If not passed, logger will be created internally.
        log_level -- Level of a log message on retry.
        exceptions_to_retry -- List of expected exceptions. `default_exceptions` is used if None.
        exceptions_to_raise -- Tuple of exceptions that should not be retried and raised
                                  immediately if fallback_value is not provided.
        fallback_value -- Return value on fait. If not provided, exception is raised.

    Attributes:
        NOT_SET -- `tools.sentinel.SentinelValue` for not provided arguments.
        default_exceptions -- A list of expected exceptions. By default, accepts all.
        default_num_tries -- Default number of tries - 5
        default_log_level -- `Logger.ERROR`
        default_fallback_value -- `NOT_SET`
        exponential_backoff -- Backoff that returns delay for each retry.
        backoff -- `exponential_backoff` is used
    """

    NOT_SET: Any = SentinelValue("NOT_SET")
    default_exceptions: Tuple[Type[BaseException], ...] = (Exception,)
    default_exceptions_to_raise: Tuple[Type[BaseException], ...] = tuple()
    default_num_tries = 5
    default_log_level = Logger.ERROR
    default_fallback_value: Any = NOT_SET

    __retry__ = True
    __backoff__ = True

    ###################
    def __init__(
        self,
        num_tries: Optional[int] = None,
        backoff: Optional[Callable[[int], int]] = None,
        logger: Optional[Logger] = None,
        log_level: Optional[int] = None,
        exceptions_to_retry: Optional[Tuple[Type[BaseException], ...]] = None,
        exceptions_to_raise: Optional[Tuple[Type[BaseException], ...]] = None,
        fallback_value: Any = NOT_SET,
    ) -> None:
        self.max_tries = max(num_tries if num_tries is not None else self.default_num_tries, 1)
        self._backoff_func = backoff if backoff is not None else self.backoff
        self._log_level = log_level if log_level is not None else self.default_log_level
        self._exceptions_to_retry = exceptions_to_retry or self.default_exceptions
        self._exceptions_to_raise = exceptions_to_raise or self.default_exceptions_to_raise
        self._lazy_logger = logger

        self._fallback_value = fallback_value
        if self._fallback_value is self.NOT_SET:
            self._fallback_value = self.default_fallback_value

    @property
    def _logger(self) -> Logger:
        if self._lazy_logger is None:
            self._lazy_logger = Logger(__name__)

        return self._lazy_logger

    @classmethod
    @contextmanager
    def no_retry(cls) -> Iterator:
        prev_retry = cls.__retry__
        cls.__retry__ = False
        try:
            yield
        finally:
            cls.__retry__ = prev_retry

    @classmethod
    @contextmanager
    def no_backoff(cls) -> Iterator:
        prev_backoff = cls.__backoff__
        cls.__backoff__ = False
        try:
            yield
        finally:
            cls.__backoff__ = prev_backoff

    @classmethod
    @contextmanager
    def translate_errors(cls) -> Iterator:
        """
        Override this function to translate errors into more specific exceptions

        See: RetryAndBackoffGsuite.translate_errors for example usage
        """
        yield

    @classmethod
    def exponential_backoff(cls, attempt_number: int) -> float:
        """
        Generate delay based on exponential backoff.

        Arguments:
            attempt_number -- Retry attempt number

        Returns:
            Delay in seconds.
        """
        return random.uniform(2 ** (attempt_number - 1), 2**attempt_number)

    @classmethod
    def backoff(cls, attempt_number: int) -> float:
        """
        Override this method in a subclass to use a different backoff generator.
        Exponential backoff generator is used.

        Arguments:
            attempt_number -- Retry attempt number

        Returns:
            Delay in seconds.
        """
        return cls.exponential_backoff(attempt_number)

    @staticmethod
    def get_exception_scope(exc: BaseException) -> Dict[str, Any]:
        """
        Get original method scope from exception object.

        Arguments:
            exc - Exception object

        Returns:
            A scope as a dict.
        """
        tb = exc.__traceback__
        if tb is None:
            return {}
        tb_next = tb.tb_next
        if tb_next is None:
            return {}
        return tb_next.tb_frame.f_locals

    def handle_exception(  # pylint: disable=no-self-use,unused-argument
        self, exc: BaseException, state: RetryState
    ) -> None:
        """
        Override this method in a subclass to do something on each failed try.
        You can access decorated method data from here.


        Arguments:
            exc -- Raised exception.
            state -- Retry state with the following attributes:
                state.method -- Decorated method.
                state.method_parent -- Decorated method parent or None if it is a function.
                state.method_args -- Arguments for method call.
                state.method_kwargs -- Keyword arguments for method call.
        """
        ...

    def _fallback(self, state: RetryState) -> Any:
        """
        Fallback when the last retry failed. Return a fallback value if it is set,
        raise expected exception otherwise.

        Returns:
            A fallback value.
        """
        if self._fallback_value is self.NOT_SET:
            if state.exception is not None:
                raise state.exception

        msg = f"Return {self._fallback_value} as a fallback."
        self._log(msg, use_defined_log_level=True)
        return self._fallback_value

    def _log(
        self,
        message: str,
        exception: Optional[BaseException] = None,
        use_defined_log_level: bool = False,
    ) -> None:
        """
        Logs a message with log_level = WARNING if the use_defined_log_level is set to False
        else logs it with defined log_level

        Arguments:
            message - message to be logged
            exception - log exception information, if any
            use_defined_log_level - flag to identify if log should be logged using defined log_level
        """
        log_level = Logger.WARNING
        if use_defined_log_level:
            log_level = self._log_level

        self._logger.log(msg=message, level=log_level)
        if exception is not None:
            self._logger.exception(exception, level=log_level)

    def __call__(self, f: FunctionType) -> FunctionType:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Per-invocation state (thread-safe):
            # (This could be local state, but that would make passing it all around more awkward.)
            state = RetryState(
                method=f,
                method_parent=args[0] if args and hasattr(args[0], f.__name__) else None,
                method_args=args,
                method_kwargs=kwargs,
                tries_remaining=self.max_tries if self.__retry__ else 1,
            )

            while state.tries_remaining > 0:
                try:
                    with self.translate_errors():
                        return f(*args, **kwargs)
                except self._exceptions_to_raise as e:
                    # don't retry the exception
                    state.tries_remaining = 0
                    state.exception = e
                    self.handle_exception(e, state)
                except self._exceptions_to_retry as e:
                    state.tries_remaining -= 1
                    state.exception = e
                    self.handle_exception(e, state)

                    if self.__backoff__ and state.tries_remaining > 0:
                        # log exception with a traceback and set delay before the next retry.
                        delay = self._backoff_func(self.max_tries - state.tries_remaining)
                        message = (
                            f"Backing off for {delay:.1f} seconds and "
                            f"retrying {state.tries_remaining} more "
                            f"{StringTools.pluralize(state.tries_remaining, 'time')}."
                        )
                        self._log(message, exception=state.exception)
                        time.sleep(delay)

            return self._fallback(state)

        return cast(FunctionType, wrapper)


#######################################
class RetryBackoffReturnNone(RetryAndBackoff):
    """
    Retry per the RetryAndBackoff class, but return None on ultimate failure.
    For example, the following will return None immediately without retrying,
    but will log message as exception and return None.

    class MyClass:
        @RetryBackoffReturnNone(exceptions_to_suppress=(ValueError,))
        def my_func(self) -> None:
            raise ValueError(f"Unable to run my_func for {self}")

    c = MyClass()
    logger.debug(f"Result: {c.my_func()}")
    """

    default_fallback_value = None
