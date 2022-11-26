from inspect import cleandoc as trim
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

from .database_type import DatabaseType


class Text(str):
    """
    Represents a literal SQL fragment. Serves the same purpose as
    <https://docs.sqlalchemy.org/en/13/core/sqlelement.html#sqlalchemy.sql.expression.text>.
    """


class Query:
    """
    Multi-database-type representation of a query string, containing separate
    versions of a query string for each type of database that requires one.
    """

    def __init__(
        self,
        query_string: Optional[str] = None,
        *,
        database_types: Optional[List[DatabaseType]] = None,
        build: Optional[Callable[[DatabaseType], Optional[str]]] = None,
        **query_strings: str,
    ) -> None:
        """
        Constructs a `StereoQuery` object containing query strings for the database
        types represented by the keyword arguments given.

        Examples:

        ```
        Query("SELECT 'any' AS foo")
        Query(build=lambda dt: f"SELECT '{dt.name.lower()}' AS foo")
        Query(mysql="SELECT 'mysql' AS foo", postgresql="SELECT 'postgresql' AS foo")
        ```

        Arguments:
            query_string: str (optional) -- Single query variant for use with *any*
                database type. If specified, database-type-specific keyword arguments or
                `build` argument should not be specified.
            database_types: List[DatabaseType] (optional) -- Database types this query
                applies to (default: all supported types).
            build: Callable (optional) -- Dynamically build query variants for the given
                database types by calling this, passing each `DatabaseType` in turn as
                an argument.
            **query_strings: str -- One keyword argument for each query variant. E.g.,
                pass a `mysql` argument for the MySQL query string, and a `postgresql`
                argument for the PostgreSQL query string.
        """
        self.database_types = database_types if database_types else list(DatabaseType)

        if query_string and query_strings:
            raise ValueError(
                f"Universal query string given, must not also specify database-type-specific "
                f"keyword arguments: {', '.join(query_strings.keys())}"
            )

        if query_strings:
            for arg_name in query_strings:
                if arg_name.upper() not in DatabaseType.__members__:
                    # A function call using an invalid call signature is a `TypeError` per
                    # <https://docs.python.org/3/library/exceptions.html#TypeError>
                    valid_arg_names = [dt.name.lower() for dt in DatabaseType]
                    raise TypeError(
                        f"Got an unexpected keyword argument '{arg_name}', "
                        f"valid keyword arguments are: {', '.join(valid_arg_names)}"
                    )

        self._build(query_string, query_strings, build)

    def _build(
        self,
        query_string: Optional[str],
        query_strings: Dict[str, str],
        build: Optional[Callable[[DatabaseType], Optional[str]]],
    ) -> None:
        self._query_strings: Dict[Optional[DatabaseType], str] = {}
        if query_string:
            # The `None` key acts as a wildcard matching *any* database types:
            self._query_strings = {None: trim(query_string)}
        elif build:
            for database_type in self.database_types:
                query_string = build(database_type)
                if query_string is not None:
                    self[database_type] = query_string
        else:
            for arg_name, value in query_strings.items():
                database_type = DatabaseType[arg_name.upper()]
                self[database_type] = value

    def __bool__(self) -> bool:
        return bool(self._query_strings)

    def __contains__(self, database_type: DatabaseType) -> bool:
        if not isinstance(database_type, DatabaseType):
            # If key is of an inappropriate type, TypeError may be raised:
            # <https://docs.python.org/3/reference/datamodel.html#object.__getitem__>
            raise TypeError(f"Key must be DatabaseType, got: {database_type!r}")
        return None in self._query_strings or database_type in self._query_strings

    def __getitem__(self, database_type: DatabaseType) -> str:
        if not isinstance(database_type, DatabaseType):
            # If key is of an inappropriate type, TypeError may be raised:
            # <https://docs.python.org/3/reference/datamodel.html#object.__getitem__>
            raise TypeError(f"Key must be DatabaseType, got: {database_type!r}")
        try:
            return self._query_strings[database_type]
        except KeyError:
            return self._query_strings[self.dominant_database_type]

    def __setitem__(self, database_type: DatabaseType, query_string: str) -> None:
        if not isinstance(database_type, DatabaseType):
            # If key is of an inappropriate type, TypeError may be raised:
            # <https://docs.python.org/3/reference/datamodel.html#object.__getitem__>
            raise TypeError(f"Key must be DatabaseType, got: {database_type!r}")
        if not isinstance(query_string, str):
            raise ValueError(f"Value must be str, got: {query_string!r}")
        self._query_strings[database_type] = trim(query_string)

    def __iter__(self) -> Iterator[Tuple[Optional[DatabaseType], str]]:
        return iter(self._query_strings.items())

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Query) and self._query_strings == other._query_strings

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._query_strings!r}>"

    @property
    def dominant_database_type(self) -> Optional[DatabaseType]:
        if not self._query_strings:
            raise ValueError("Query string for at least one database type required, got none")
        return next(iter(self._query_strings))


QueryT = Union[Query, str]
