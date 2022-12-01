import os
from logging import Formatter, LogRecord, StrFormatStyle
from string import Formatter as StrFormatter
from typing import Any, Dict, Optional, Pattern, Type, TypeVar

from pytools.common import json_utils

_R = TypeVar("_R", bound="LoggerJSONFormatter")


class JSONStrFormatStyle(StrFormatStyle):
    """
    Extended StrFormatStyle.

    Adds a few more variables:

    - `{stackinfo}`
    """

    ALLOWED_FIELDS = (
        "name",
        "levelno",
        "levelname",
        "pathname",
        "filename",
        "module",
        "lineno",
        "funcName",
        "created",
        "asctime",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "process",
        "message",
        "group",
        "stackinfo",
    )
    field_spec: Pattern[str]
    fmt_spec: Pattern[str]

    def __init__(self, fmt: Optional[str] = None) -> None:
        super().__init__("")
        self._dict_fmt = json_utils.loads(fmt or "{}")
        self._str_formatter = StrFormatter()

    def validate(self) -> None:
        """Validate the input format, ensure it is the correct string formatting style"""
        for value in self._dict_fmt.values():
            if not isinstance(value, str):
                continue

            for _, fieldname, spec, conversion in self._str_formatter.parse(value):
                if fieldname and fieldname not in self.ALLOWED_FIELDS:
                    raise ValueError(f"unknown format key: {{{fieldname}}} in {self._dict_fmt}")
                if conversion and conversion not in "rsa":
                    raise ValueError(f"invalid conversion: {conversion} in {self._dict_fmt}")
                if spec and not self.fmt_spec.match(spec):
                    raise ValueError(f"bad specifier: {spec} in {self._dict_fmt}")

    def format(self, record: LogRecord) -> str:
        result = {}
        format_dict = {
            "group": None,
            "stackinfo": str(record.stack_info) if record.stack_info else "",
            **record.__dict__,
        }
        for key, value in self._dict_fmt.items():
            if not isinstance(value, str):
                result[key] = value
                continue
            result[key] = value.format(**format_dict)
        return json_utils.dumps(result, sort_keys=False, indent=None)


class LoggerJSONFormatter(Formatter):
    """
    Formatter that produces JSON messages.

    Arguments:
        fmt -- JSON-serializeable format string
        datefmt -- Desired asctime format
        style -- Not used, left for compatibility.
        validate -- Whether to check for unknown format keys.

    Examples:

        ```python
        logger = Logger("test", formatter=LoggerJSONFormatter())
        logger.error("my_error")

        {
            "asctime": "2020-08-03 21:23:26",
            "message": "my_error",
            "pathname": "path/my_module.py",
            "funcName": "my_func",
            "lineno": "123",
            "name": "test",
            "group": "app",
            "level": "ERROR",
            "env": "dev",
        }
        ```
    """

    default_dict_format: Dict[str, Any] = {
        "asctime": "{asctime}",
        "message": "{message}",
        "pathname": "{pathname}",
        "funcname": "{funcName}",
        "lineno": "{lineno}",
        "name": "{name}",
        "group": "{group}",
        "level": "{levelname}",
        "env": os.getenv("ENV"),
    }
    default_msec_format = "%s%00d"

    # pylint: disable=super-init-not-called
    def __init__(
        self,
        fmt: Optional[str] = None,
        datefmt: Optional[str] = None,
        style: str = "{",
        validate: bool = True,
    ) -> None:
        self._style: JSONStrFormatStyle
        if not fmt:
            fmt = json_utils.dumps(self.default_dict_format)

        self.default_format = fmt
        self.dict_format = json_utils.loads(fmt)
        self._style = JSONStrFormatStyle(fmt)

        if validate:
            self._style.validate()

        self._fmt = self._style._fmt
        self.fmt = self._fmt
        self.datefmt = datefmt or "%Y-%m-%d %H:%M:%S"

    @classmethod
    def create(cls: Type[_R], data: Dict[str, Any], datefmt: Optional[str] = None) -> _R:
        """
        Create a formatter from Dict structure.

        Arguments:
            data -- JSON-serializeable dict with format-ready strings as values
            datefmt -- Preferred dateformat.

        Examples:

            ```python
            LoggerJSONFormatter.create({
                "key": "value",
                "message": "{message}",
            })
            ```
        """
        return cls(fmt=json_utils.dumps(data), datefmt=datefmt)

    def format(self, record: LogRecord) -> str:
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)
        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        s = self.formatMessage(record)
        return s
