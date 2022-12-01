import time
from types import TracebackType
from typing import Callable, Optional, Type, Union
from warnings import warn

from pytools.common.logger import Logger


class Timer:
    def __init__(self, logger: Optional[Union[Logger, bool]] = None) -> None:
        self._start: Optional[float] = None
        self._end: Optional[float] = None
        self._duration: Optional[float] = None
        self._logger = Logger(__name__) if logger is True else logger

    def __enter__(self) -> "Timer":
        if self._start is not None:
            raise RuntimeError(
                f"{self.__class__} is not reusable; use fresh {self.__class__} instead"
            )
        self._start = time.monotonic()
        return self

    def __exit__(
        self,
        e_type: Optional[Type[BaseException]],
        e: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        assert self._start is not None
        self._end = time.monotonic()
        self._duration = self._end - self._start
        if self._logger:
            self._logger.info(f"Execution completed in {self._duration} seconds")  # type: ignore

    @property
    def duration(self) -> Optional[float]:
        return self._duration


class SlowWarning(Warning):
    def __init__(self, timer: "SlowTimer") -> None:
        self.timer = timer
        self.title = timer.message
        self.message = timer.slow_message
        super().__init__(self.message)

    def as_sentry_event(self) -> dict:
        return {
            "level": "warning",
            "title": self.title,
            "message": self.message,
            "extra": {
                "duration": self.timer.duration,
                "slow_threshold": self.timer.slow_threshold,
            },
        }


class SlowTimer:
    def __init__(
        self,
        slow_threshold: float,
        message: str = "Slow operation",
        make_warning: Callable[..., SlowWarning] = SlowWarning,
    ) -> None:
        self.slow_threshold = slow_threshold
        self.message = message
        self.make_warning = make_warning

        self._start: Optional[float] = None
        self._end: Optional[float] = None
        self._duration: Optional[float] = None

    def __enter__(self) -> "SlowTimer":
        if self._start is not None:
            raise RuntimeError(
                f"{self.__class__} is not reusable; use fresh {self.__class__} instead"
            )
        self._start = time.monotonic()
        return self

    def __exit__(
        self,
        e_type: Optional[Type[BaseException]],
        e: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        assert self._start is not None
        self._end = time.monotonic()
        self._duration = self._end - self._start
        if self._duration >= self.slow_threshold:
            self.on_slow()

    @property
    def start_time(self) -> Optional[float]:
        return self._start

    @property
    def end_time(self) -> Optional[float]:
        return self._end

    @property
    def duration(self) -> Optional[float]:
        return self._duration

    @property
    def slow_message(self) -> str:
        self._assert_slow()
        return f"{self.message} ({self.duration:.3f}s >= {self.slow_threshold:.3f}s)"

    @property
    def warning(self) -> SlowWarning:
        self._assert_slow()
        return self.make_warning(self)

    def on_slow(self) -> None:
        warn(self.warning)

    def _assert_slow(self) -> None:
        if self._duration is None:
            raise RuntimeError(f"{self.__class__} context has not completed")
        if self._duration < self.slow_threshold:
            raise ValueError("Operation was not slow")
