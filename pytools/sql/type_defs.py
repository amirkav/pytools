import enum
from typing import Any, Dict, Tuple, Union


QueryParams = Union[Dict[str, Any], Tuple]


class DatabaseType(enum.Enum):
    """
    SQL database type used by SQLConnect and Config.
    """

    MYSQL = enum.auto()
    POSTGRESQL = enum.auto()
