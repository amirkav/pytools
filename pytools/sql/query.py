from inspect import cleandoc as trim
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Union


# TODO: Do we even need this module??


class Query:
    """
    Representation of a query string for PostgreSQL.
    """

    database_type = "postgresql"

    def __init__(
        self,
        query_string: Optional[str] = None,
        *,
        build: Optional[Callable[[str], Optional[str]]] = None,
    ) -> None:
        """
        Constructs a `Query` object containing query strings.

        Examples:

        ```
        Query("SELECT 'any' AS foo")
        Query(build=lambda dt: f"SELECT '{dt.name.lower()}' AS foo")
        ```

        Arguments:
            query_string: str (optional) -- Single query variant.
                If specified, database-type-specific keyword arguments or
                `build` argument should not be specified.
            build: Callable (optional) -- Dynamically build the query string
        """

        self._build(query_string, build)

    def _build(
        self,
        query_string: Optional[str],
        build: Optional[Callable[[str], Optional[str]]],
    ) -> None:
        if query_string:
            self._query_string = {self.database_type: trim(query_string)}
        elif build:
            query_string = build()
            if query_string is not None:
                self._query_string = query_string

    def __bool__(self) -> bool:
        return bool(self._query_string)

    def __contains__(self) -> bool:
        return None in self._query_string

    def __getitem__(self) -> str:
        return self._query_string[self.database_type]

    def __setitem__(self, query_string: str) -> None:
        if not isinstance(query_string, str):
            raise ValueError(f"Value must be str, got: {query_string!r}")
        self._query_string[self.database_type] = trim(query_string)

    def __iter__(self) -> Iterator[Tuple[Optional[str], str]]:
        return iter(self._query_string.items())

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Query) and self._query_string == other._query_string

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {self._query_string!r}>"


QueryT = Union[Query, str]
