import logging
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Type, Union
from unittest.mock import patch

from pytools.common import json_utils
from pytools.common.logger_json_formatter import LoggerJSONFormatter

__all__ = ("Logger", "LoggerError")


class LoggerError(Exception):
    """
    Default error for `Logger`.
    """


lock = threading.RLock()


# pylint: disable=super-init-not-called,arguments-differ
class Logger(logging.Logger):
    """
    Wrapper on top of `logging.Logger`. Has Stream and File handlers.
    Stream handler logs to console. File handler logs to `<temp_dir>/app_log.log`.

    Examples:

        ```python
        # Basic usage
        logger = Logger('my_logger')
        logger.info('I am using logging.INFO level')
        logger.log(msg='This is identical to logger.error call', level=Logger.ERROR)
        logger.json({'key': 'value'}, level=Logger.INFO)

        # Real-life example
        logger = Logger(__name__, level=Logger.WARNING)
        logger.warning('I use current filename with no extension as logger name')
        logger.error('Exception info is supported as well',
                    exc_info=ValueError('test'))

        # JSON-format example
        logger = Logger("json_logger", formatter=Logger.JSONFormatter())

        try:
            data = json.loads('{invalid json}')
        except Exception as e:
            logger.exception(e)
        ```

    Arguments:
        name -- Name of logger. If logger already exists - returns existing.
        level -- Log level, works the same as `logging` built-in levels.
        group -- Log group level.
        log_file_path -- Path to log file, set to `None` to turn off `FileHandler`.
        formatter -- logging.Formatter instance.

    Attributes:
        DEBUG -- Alias for `logging.DEBUG`.
        INFO -- Alias for `logging.INFO`.
        WARNING -- Alias for `logging.WARNING`.
        ERROR -- Alias for `logging.ERROR`.
        CRITICAL -- Alias for `logging.CRITICAL`.
        NOTSET -- Alias for `logging.NOTSET`.
        LOG_FILE_PATH -- Path to `logging.FileHandler` file.
        DATETIME_FORMAT -- Strftime-style datetime format string.
        FORMAT -- Log message format.
        DEFAULT_GROUP -- Default logger group name: `app`
        MAIN_LOGGER_NAME -- Default name for main logger: `__main__`
        ENABLE_STACK_INFO -- Add stack info to log records
        SRCFILES -- List of files to ignore in trace info
        JSONFormatter -- Shortcut for using JSON formatter
    """

    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
    NOTSET = logging.NOTSET
    LEVEL_TO_NAME = logging._levelToName  # pylint: disable=protected-access
    ENABLE_STACK_INFO = False
    SRCFILES = (os.path.normcase(__file__),)

    LOG_FILE_PATH = Path(tempfile.gettempdir(), "app_log.log")
    FORMATTER_CLASS: Type[logging.Formatter] = logging.Formatter
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    FORMAT = "%(asctime)s %(name)s %(group)s: %(levelname)-8s %(message)s"
    DEFAULT_GROUP = "app"
    MAIN_LOGGER_NAME = "__main__"

    JSONFormatter = LoggerJSONFormatter

    _logger_dict: Dict[str, "Logger"] = dict()
    _logging_srcfile = getattr(logging, "_srcfile", "")
    _logging_os_path = logging.os.path  # type: ignore

    def __repr__(self) -> str:
        level_name = self.LEVEL_TO_NAME.get(self.level)
        return f'<Logger {self.name} group="{self.group}" ({level_name})>'

    def __init__(
        self,
        name: str,
        level: int = NOTSET,
        *,
        group: str = DEFAULT_GROUP,
        log_file_path: Union[Path, str, None] = LOG_FILE_PATH,
        formatter: Optional[logging.Formatter] = None,
        is_main: bool = False,
    ) -> None:
        if hasattr(self, "name"):
            return
        self.name = name
        self._group = group
        self._level = level
        self._is_main = is_main
        self._log_file_path = None
        self.formatter = formatter
        if log_file_path is not None:
            self._log_file_path = Path(log_file_path)

    def _get_formatter(self, default: Optional[logging.Formatter] = None) -> logging.Formatter:
        if self.formatter:
            return self.formatter

        if default:
            return default

        return self.FORMATTER_CLASS(fmt=self.FORMAT, datefmt=self.DATETIME_FORMAT)

    @classmethod
    def normcase_patch(cls, path: str) -> str:
        """
        Patch for `os.path.normpath` to exclude `SRCFILES` from trace info.
        """
        if path in cls.SRCFILES:
            return cls._logging_srcfile

        return path

    def __new__(
        cls,
        name: str,
        level: int = NOTSET,
        group: str = DEFAULT_GROUP,
        log_file_path: Union[Path, str, None] = LOG_FILE_PATH,
        formatter: Optional[logging.Formatter] = None,
        is_main: bool = False,
    ) -> "Logger":
        cls._validate_level(level)
        if name in cls._logger_dict:
            existing_logger = cls._logger_dict[name]
            if is_main:
                existing_logger.warning(
                    f"Attempt to override main logger '{name}', ignoring changes."
                )
            return existing_logger

        main_logger = cls._get_main_logger()
        if main_logger and is_main:
            main_logger.warning(
                f"Attempt to create '{name}' extra main logger while '{main_logger.name}' exists."
                f" '{main_logger.name}' logger is still considered as main."
            )
            is_main = False

        if main_logger:
            if group == cls.DEFAULT_GROUP:
                group = main_logger.group
            if level == cls.NOTSET:
                level = main_logger.level

        result = object.__new__(cls)
        result.__init__(
            name=name,
            group=group,
            level=level,
            log_file_path=log_file_path,
            formatter=formatter,
            is_main=is_main,
        )
        result.create_logger()

        if is_main:
            result.fix_existing_loggers()

        cls._logger_dict[name] = result
        return result

    @classmethod
    def main(
        cls,
        name: str = "__main__",
        level: int = NOTSET,
        group: str = DEFAULT_GROUP,
        log_file_path: Union[Path, str, None] = LOG_FILE_PATH,
        formatter: Optional[logging.Formatter] = None,
    ) -> "Logger":
        """
        Create main logger instance.

        Arguments:
            name -- Logger name, by default `__main__`
            level -- Log level, works the same as `logging` built-in levels.
            group -- Log group level.
            log_file_path -- Path to log file, set to `None` to turn off `FileHandler`.
            formatter -- logging.Formatter instance.

        Returns:
            Logger instance with name `name`.
        """
        return cls(
            name=name,
            group=group,
            level=level,
            log_file_path=log_file_path,
            formatter=formatter,
            is_main=True,
        )

    @classmethod
    def get_log_level_from_env(cls) -> int:
        """
        Get default log level from environment.

        If `DEBUG_MODE` env variable is set to `true`, returns `DEBUG`,
        else `INFO`.

        Returns:
            Log level as integer.
        """
        if os.getenv("DEBUG_MODE", "false") == "true":
            return cls.DEBUG

        return cls.INFO

    @classmethod
    def for_object(
        cls,
        obj: Any,
        level: int = NOTSET,
        group: str = DEFAULT_GROUP,
        log_file_path: Union[Path, str, None] = LOG_FILE_PATH,
        formatter: Optional[logging.Formatter] = None,
    ) -> "Logger":
        """
        Create main logger instance.

        Arguments:
            obj -- Class instance
            level -- Log level, works the same as `logging` built-in levels.
            group -- Log group level.
            log_file_path -- Path to log file, set to `None` to turn off `FileHandler`.
            formatter -- logging.Formatter instance.

        Returns:
            Logger instance with name as fully qualified obj path.
        """
        return cls(
            name=".".join([obj.__module__, obj.__class__.__name__]),
            group=group,
            level=level,
            log_file_path=log_file_path,
            formatter=formatter,
        )

    @classmethod
    def _get_logger(cls, name: str) -> Optional["Logger"]:
        """
        Get existing logger from cache.

        Returns:
            Existing Logger instance or None.
        """
        return cls._logger_dict.get(name)

    def is_main(self) -> bool:
        """
        Check if current logger is created from main endpoint.

        Returns:
            True of Logger has name `__main__`
        """
        return self._is_main

    def set_formatter(self, formatter: Optional[logging.Formatter]) -> None:
        """
        Set formatter for all handlers.
        """
        self.formatter = formatter
        handler_formatter = self._get_formatter()
        for handler in self.handlers:
            handler.setFormatter(handler_formatter)

    @classmethod
    def _validate_level(cls, level: int) -> None:
        if level not in cls.LEVEL_TO_NAME:
            raise LoggerError(f"Unknown log level: {level}")

    def create_logger(self) -> None:
        """
        Create underlying `logging.Logger`
        """
        self._logger = logging.Logger(self.name)
        self._logger_adapter = logging.LoggerAdapter(self._logger, extra={"group": self.group})
        main_logger = self._get_main_logger()
        if main_logger is None or self.is_main():
            self._create_handlers()
            return

        self.copy_handlers(main_logger.handlers)

    @classmethod
    def _get_main_logger(cls) -> Optional["Logger"]:
        for existing_logger in cls._logger_dict.values():
            if existing_logger.is_main():
                return existing_logger

        return None

    def fix_existing_loggers(self) -> None:
        for child_logger in self._logger_dict.values():
            if child_logger.is_main():
                continue
            child_logger.copy_handlers(self.handlers)
            if child_logger.real_level == self.NOTSET:
                child_logger.level = self.level
            if child_logger.group == self.DEFAULT_GROUP:
                child_logger.group = self.group

    def copy_handlers(self, handlers: Iterable[logging.Handler]) -> None:
        """
        Copy Logger handlers from another Logger.

        Arguments:
            handlers -- List of Logger.handlers
        """
        self._logger.handlers = []
        for handler in handlers:
            new_handler: logging.Handler = handler
            if isinstance(handler, logging.FileHandler):
                new_handler = logging.FileHandler(handler.baseFilename)
            elif isinstance(handler, logging.StreamHandler):
                new_handler = logging.StreamHandler()

            formatter = self._get_formatter(handler.formatter)
            new_handler.setFormatter(formatter)

            new_handler.setLevel(handler.level)
            self._logger.handlers.append(new_handler)

    def _create_handlers(self) -> None:
        # set a format which is simpler for console use
        self._logger.setLevel(self.level)

        # define a Handler which writes messages to the sys.stderr
        console = logging.StreamHandler()
        formatter = self._get_formatter()
        console.setFormatter(formatter)
        console.setLevel(self.level)
        self._logger.addHandler(console)

        # define a Handler which writes messages to a log file specified by file_path
        if self._log_file_path:
            self._log_file_path.touch()
            file_handler = logging.FileHandler(self._log_file_path)
            file_handler.setFormatter(formatter)
            file_handler.setLevel(self.level)
            self._logger.addHandler(file_handler)

    @property
    def handlers(self) -> List[logging.Handler]:  # type: ignore
        """
        Get a list of logger handlers.

        Returns:
            List of logging handlers.
        """
        return self._logger.handlers

    @property
    def real_level(self) -> Optional[int]:
        """
        Get current log level that was passed on logger create.

        Returns:
            Log level as an integer.
        """
        return self._level

    @property
    def group(self) -> str:
        """
        Get current group.

        Returns:
            Group name.
        """
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        """
        Set current group.

        Returns:
            Group name.
        """
        self._group = group
        extra = dict(self._logger_adapter.extra, group=group)
        self._logger_adapter.extra = extra

    @property  # type: ignore
    def level(self) -> int:  # type: ignore
        """
        Get current log level.

        Returns:
            Log level as an integer.
        """
        if self._level:
            return self._level

        return self.get_log_level_from_env()

    @level.setter
    def level(self, level: int) -> None:
        """
        Set level of logger and all of it's handlers.

        Arguments:
            level -- Log level.
        """
        with lock:
            self._validate_level(level)
            log_level = level
            if log_level == Logger.NOTSET:
                log_level = self.get_log_level_from_env()
            self._logger.setLevel(log_level)
            for handler in self._logger.handlers:
                handler.setLevel(log_level)
            self._level = level

    def debug(self, message: str, exc_info: Optional[BaseException] = None) -> None:  # type: ignore
        """
        Alias for `logging.debug`.

        Arguments:
            message -- Log message.
            exc_info -- Provide Exception to add to log message.
        """
        with lock:
            with patch.object(self._logging_os_path, "normcase", self.__class__.normcase_patch):
                self._logger_adapter.debug(
                    message, exc_info=exc_info, stack_info=self.ENABLE_STACK_INFO
                )

    def info(self, message: str, exc_info: Optional[BaseException] = None) -> None:  # type: ignore
        """
        Alias for `logging.info`.

        Arguments:
            message -- Log message.
            exc_info -- Provide Exception to add to log message.
        """
        with lock:
            with patch.object(self._logging_os_path, "normcase", self.__class__.normcase_patch):
                self._logger_adapter.info(
                    message, exc_info=exc_info, stack_info=self.ENABLE_STACK_INFO
                )

    def warning(  # type: ignore
        self, message: str, exc_info: Optional[BaseException] = None
    ) -> None:
        """
        Alias for `logging.warning`.

        Arguments:
            message -- Log message.
            exc_info -- Provide Exception to add to log message.
        """
        with lock:
            with patch.object(self._logging_os_path, "normcase", self.__class__.normcase_patch):
                self._logger_adapter.warning(
                    message, exc_info=exc_info, stack_info=self.ENABLE_STACK_INFO
                )

    def error(self, message: str, exc_info: Optional[BaseException] = None) -> None:  # type: ignore
        """
        Alias for `logging.error`.

        Arguments:
            message -- Log message.
            exc_info -- Provide Exception to add to log message.
        """
        with lock:
            with patch.object(self._logging_os_path, "normcase", self.__class__.normcase_patch):
                self._logger_adapter.error(
                    message, exc_info=exc_info, stack_info=self.ENABLE_STACK_INFO
                )

    def critical(  # type: ignore
        self, message: str, exc_info: Optional[BaseException] = None
    ) -> None:
        """
        Alias for `logging.critical`.

        Arguments:
            message -- Log message.
            exc_info -- Provide Exception to add to log message.
        """
        with lock:
            with patch.object(self._logging_os_path, "normcase", self.__class__.normcase_patch):
                self._logger_adapter.critical(
                    message, exc_info=exc_info, stack_info=self.ENABLE_STACK_INFO
                )

    def log(  # type: ignore
        self, level: int, msg: str, exc_info: Optional[BaseException] = None
    ) -> None:
        """
        Alias for `logging.log`.

        Arguments:
            message -- Log message.
            level -- Log message level.
            exc_info -- Provide Exception to add to log message.
        """
        method_map = {
            self.DEBUG: self.debug,
            self.INFO: self.info,
            self.WARNING: self.warning,
            self.ERROR: self.error,
            self.CRITICAL: self.critical,
        }
        if level not in method_map:
            raise LoggerError(f"Unknown log level: {level}")

        method = method_map[level]
        method(msg, exc_info)

    def exception(  # type: ignore
        self,
        exception: BaseException,
        level: int = ERROR,
        show_traceback: bool = True,
    ) -> None:
        """
        Shortcut for logging exception message with traceback.

        Arguments:
            exception -- Exception to log.
            level -- Level of log message.
        """
        message = f"{exception.__class__.__name__}: {exception}"
        exc_info = exception if show_traceback else None

        self.log(msg=message, level=level, exc_info=exc_info)

    def json(
        self, data: Any, level: int = DEBUG, name: str = "", indent: Optional[int] = None
    ) -> None:
        """
        Log any JSON-serializeable object.

        Arguments:
            data -- JSON-serializeable object.
            level -- Level of log message.
            name -- Name of JSON data.
        """
        message = json_utils.dumps(data, indent=indent)
        if name:
            message = f"{name} = {message}"
        self.log(msg=message, level=level)

    @staticmethod
    def _escape_brackets(value: Any) -> Any:
        if not isinstance(value, str):
            return value

        return value.replace("{", "{{").replace("}", "}}")

    def json_custom_attributes(
        self, data: Dict[str, Any], level: int = DEBUG, name: str = ""
    ) -> None:
        """
        Log any JSON-serializeable object adding it to JSOn formatter message.

        Falls back to `json` if JSONFormatter is not used.
        Data values with curly brackets are escaped to avoid unnecessary formatting.

        Arguments:
            data -- Formatter attributes.
            level -- Log level.
            name -- JSON structure name.
        """
        if not isinstance(self.formatter, Logger.JSONFormatter):
            self.json(data, level=level, name=name)
            return

        with lock:
            old_formatter = self.formatter
            safe_data = {key: self._escape_brackets(value) for key, value in data.items()}
            new_formatter = Logger.JSONFormatter.create({**old_formatter.dict_format, **safe_data})
            self.set_formatter(new_formatter)
            self.log(msg=name or "JSON", level=level)
            self.set_formatter(old_formatter)

    def has_json_formatter(self) -> bool:
        """
        Whether Logger uses a LoggerJSONFormatter formatter.

        Returns:
            True or False.
        """
        if self.formatter:
            return isinstance(self.formatter, LoggerJSONFormatter)

        main_logger = self._get_main_logger()
        if not main_logger:
            return False

        main_formatter = main_logger.formatter
        return isinstance(main_formatter, LoggerJSONFormatter)


def main() -> None:
    old_logger = Logger("old")
    main_logger = Logger.main(group="test_loggers")
    new_logger = Logger("new", level=Logger.WARNING)

    main_logger.debug(str(old_logger))
    main_logger.debug(str(main_logger))
    main_logger.debug(str(new_logger))
    main_logger.json({"key": 123}, level=Logger.ERROR)

    old_logger.warning("old_logger")
    main_logger.warning("main_logger")
    new_logger.warning("new_logger")

    class LoggerDemo:
        def __init__(self) -> None:
            self.logger = Logger.for_object(self)
            self.logger.info("Fully qualified class name logger")
            self.logger.debug("Fully qualified class name logger")
            self.logger.warning("Fully qualified class name logger")

        def my_method(self) -> None:
            self.logger.info("hello")

    LoggerDemo().my_method()

    # l = Logger("json", formatter=Logger.JSONFormatter())
    # l.info("test")
    # l.set_formatter(Logger.JSONFormatter.create({"key": "value"}))
    # l.info("test2")


if __name__ == "__main__":
    main()
