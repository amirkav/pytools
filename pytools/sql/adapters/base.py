import dataclasses
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from pytools.common.class_tools import cached_property
from pytools.common.dynamic_namespace import DynamicNamespace

from .. import pep249
from ..type_defs import DatabaseType, QueryParams
from ..query import Query, QueryT
from ..route import Route

# Types
# =============================================================================

ConnectionT = TypeVar("ConnectionT", bound=pep249.Connection)
CursorT = TypeVar("CursorT", bound=pep249.Cursor)

ExecuteContextManagerFactory = Callable[..., ContextManager]


# Query execution base classes
# =============================================================================


class Rollback(Exception):
    """Trigger a rollback inside a transaction."""


class IsolationLevel(Enum):
    """
    Transaction isolation levels.

    MySQL: <https://dev.mysql.com/doc/refman/5.7/en/innodb-transaction-isolation-levels.html>
    PostgreSQL: <https://www.postgresql.org/docs/current/transaction-iso.html>
    """

    READ_UNCOMMITTED = auto()
    READ_COMMITTED = auto()
    REPEATABLE_READ = auto()
    SERIALIZABLE = auto()


# QueryPlan base classes
# =============================================================================


@dataclass
class QueryPlanNode:
    """Generic query plan node."""

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclass
class QueryPlan:
    """Generic query plan."""

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


# DatabaseAdapter base class
# =============================================================================


class DatabaseAdapter(Generic[ConnectionT, CursorT], ABC):
    """
    Generic database adapter class.

    Subclass this for specific database functionality.
    """

    # `CursorT` has an upper bound of `pep249.Cursor`, and `pep249.{Tuple,Dict}Cursor` are
    # subclasses of `pep249.Cursor`, so this is safe. Unclear why mypy even requires `cast`.
    TupleCursor: Type[CursorT] = cast(Type[CursorT], pep249.TupleCursor)
    DictCursor: Type[CursorT] = cast(Type[CursorT], pep249.DictCursor)

    # Registry mapping database types to adapter classes:
    # TODO: we may not need this.
    adapter_class_by_type: Dict[DatabaseType, Type["DatabaseAdapter"]] = {}

    database_type: DatabaseType

    # Class methods
    # =========================================================================

    # pylint: disable=arguments-differ
    def __init_subclass__(cls, *, database_type: DatabaseType) -> None:
        super().__init_subclass__()
        cls.database_type = database_type
        cls.adapter_class_by_type[database_type] = cls

    # TODO: remove all of this complexity. We don't need to have this level of code flexibility and mobility. Assume we use one type of database (postgres)
    @classmethod
    def adapter_class(cls, database_type: DatabaseType) -> Type["DatabaseAdapter"]:
        try:
            return cls.adapter_class_by_type[database_type]
        except KeyError:
            raise ValueError(f"Unknown database type: {database_type}") from None

    # Methods
    # =========================================================================

    def __init__(
        self,
        route: Route,
        *,
        autocommit: bool,
        cursor_class: Type[CursorT],
        execute_contextmanager: Optional[ExecuteContextManagerFactory] = None,
    ) -> None:
        """
        Initialize a database adapter.

        Arguments:
            route: Route -- Database type, connection, and authentication information.
            autocommit: bool -- Whether to enable autocommit mode (default: False).
            cursor_class: Type[Cursor] -- Cursor class to use by default when fetching query
                results. Possible cursor classes are database-specific. (Default: use
                database driver's tuple cursor.)
            execute_contextmanager: Callable[..., ContextManager] (optional) -- When
                specified, wrap each low-level database `execute` call in a context returned
                by this callable. Must accept the following arguments:
                    cursor: Cursor -- Cursor instance that will be used to execute statement
                    query_string: str -- The SQL query to execute.
                    query_params: tuple | dict (optional) -- Query parameters.
                    **kwargs -- Any other keyword arguments that were passed in the adapter
                        call.
        """
        self.route = route
        self.autocommit = autocommit
        self.cursor_class: Type[CursorT] = self._concrete_cursor_class(cursor_class)
        self.execute_contextmanager = execute_contextmanager

        self._connection: Optional[ConnectionT] = None
        self._transaction_level: int = 0

    def _concrete_cursor_class(self, cursor_class: Type[CursorT]) -> Type[CursorT]:
        if cursor_class is pep249.TupleCursor:
            cursor_class = self.TupleCursor
        elif cursor_class is pep249.DictCursor:
            cursor_class = self.DictCursor
        return cursor_class

    def _query_string(self, query_string: QueryT) -> str:
        return query_string[self.database_type] if isinstance(query_string, Query) else query_string

    @abstractmethod
    def connect(self) -> ConnectionT:
        """
        Establish a fresh database connection.

        Refresh the route's IAM token if necessary.

        Retry on failure.

        Returns:
            Database connection.
        """

    @property
    def connection(self) -> ConnectionT:
        """
        Establish a database connection if one is not already open.

        Returns:
            Database connection.
        """
        if self._connection:
            return self._connection
        if self.route.expired:
            self.route.refresh_iam_token()
        return self.connect()

    def close(self) -> None:
        """Close database connection if one has been opened."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @property
    @abstractmethod
    def is_open(self) -> bool:
        """Returns whether a database connection has been opened and is usable."""

    @abstractmethod
    def quote_ident(self, name: str) -> str:
        """Returns a quoted representation of the given identifier."""

    @abstractmethod
    def connection_count_for_database(self, database: Optional[str] = None) -> int:
        """
        Returns the number of database connections as seen from the database system.

        Arguments:
            database: str (optional) -- If specified, count only connections opened to the
                database with the given name. For the current database, specify
                `route.database`.
        """

    @property
    def connection_count(self) -> int:
        """Returns the number of database connections as seen from the database system."""
        return self.connection_count_for_database()

    @abstractmethod
    def _getvar(self, path: List[str]) -> Any:
        """
        Get the value of the given database variable.

        Arguments:
            path: List[str] -- Path of the database variable to get.

        Returns:
            Value of the database variable.
        """

    def getvar(self, name: str) -> Any:
        """
        Get the value of the given database variable.

        Arguments:
            name: str -- Name of the database variable to get.

        Returns:
            Value of the database variable.
        """
        return self._getvar([name])

    @abstractmethod
    def _setvar(self, path: List[str], value: Any) -> None:
        """
        Set the value of the given database variable.

        Arguments:
            path: List[str] -- Path of the database variable to set.
            value: Any -- Value to assign to the database variable.
        """

    def setvar(self, name: str, value: Any) -> None:
        """
        Set the value of the given database variable.

        Arguments:
            name: str -- Name of the database variable to set.
            value: Any -- Value to assign to the database variable.
        """
        return self._setvar([name], value)

    @cached_property
    def vars(self) -> DynamicNamespace:
        """
        A namespace that gives direct read and write access to database variables.

        For example:

        >>> database_adapter.vars.version
        '5.7.12'
        >>> database_adapter.vars.autocommit
        False
        >>> database_adapter.vars.autocommit = True
        >>> database_adapter.vars.autocommit
        True
        """
        return DynamicNamespace(self._getvar, self._setvar)

    @contextmanager
    def setvars(self, variables: Optional[dict] = None, **kwvars: Any) -> Iterator:
        """
        Context manager that sets the given database variables to the given values for the
        duration of the context and restores them to their original values afterwards.

        Nestable.

        Variables may be passed as a `dict` or as keyword arguments.

        For example:

        >>> with database_adapter.setvars(autocommit=True):
        ...     database_adapter.execute("UPDATE ...")

        Arguments:
            variables: dict (optional) -- Variable name/value pairs as a `dict`.
            kwargs (optional) -- Variable name/value pairs as keyword arguments.
        """
        if variables is None:
            variables = {}
        variables.update(kwvars)

        prev_vars = {}
        for name, value in variables.items():
            prev_vars[name] = self.getvar(name)
            self.setvar(name, value)

        yield self

        for name, value in prev_vars.items():
            self.setvar(name, value)

    @cached_property
    @abstractmethod
    def server_version(self) -> Optional[Tuple[int, ...]]:
        """
        Returns the server version as a tuple of integers for easy comparison in Python
        code.

        For example:

        >>> if database_adapter.server_version >= (x, y):
        ...     # use new feature in database version x.y
        """

    @cached_property
    @abstractmethod
    def identifier_quote_char(self) -> str:
        """Returns the quote character recognized by the database."""

    @property
    @abstractmethod
    def connection_id(self) -> int:
        """Returns the unique ID of the database connection."""

    @property
    @abstractmethod
    def transaction_id(self) -> Optional[int]:
        """Returns the unique ID of the current transaction, if any."""

    @property
    @abstractmethod
    def transaction_isolation(self) -> IsolationLevel:
        """Returns the current session's transaction isolation level."""

    @property
    def transaction_level(self) -> int:
        """
        Returns the nesting level of `transaction` contexts.

        For example:

        >>> assert database_adapter.transaction_level == 0
        >>> with database_adapter.transaction():
        ...     assert database_adapter.transaction_level == 1
        ...     with database_adapter.transaction():
        ...         assert database_adapter.transaction_level == 2
        """
        return self._transaction_level

    @contextmanager
    @abstractmethod
    def transaction(self, isolation_level: Optional[IsolationLevel] = None) -> Iterator:
        """
        Execute a context inside a database transaction. Automatically commit unless an
        unhandled exception occurs, in which case automatically rolls back and re-raises.

        For example:

        >>> with database_adapter.transaction():
        ...     database_adapter.execute("UPDATE ...")

        Most database systems do not support nested transactions, however this may still be
        nested. This has no special semantics other than incrementing `transaction_level`.

        If you wish to roll back the current transaction without raising an unhandled
        exception, raise the `Rollback` pseudo-exception. This will trigger a rollback
        without re-raising anything.

        Example:

        >>> with database_adapter.transaction():
        ...     if data_current:
        ...         ...
        ...     else:
        ...         raise Rollback

        Arguments:
            isolation_level: IsolationLevel (optional) -- Execute the transaction with the
                given isolation level. See the `IsolationLevel` enum class for the supported
                isolation levels.
        """

    def commit(self) -> None:
        """
        Explicitly commit an ongoing transaction. For most intents and purposes, use the
        `transaction` context manager instead.
        """
        self.connection.commit()

    def rollback(self) -> None:
        """
        Explicitly roll back an ongoing transaction. For most intents and purposes, use the
        `transaction` context manager and raise the `Rollback` pseudo-exception instead.
        """
        self.connection.rollback()

    @abstractmethod
    def cursor(self, cursor_class: Optional[Type[CursorT]] = None) -> CursorT:
        """
        Create a database cursor. By default, instantiates the cursor class specified in
        the constructor's `cursor_class` argument.

        Arguments:
            cursor_class: Type[Cursor] (optional) -- If specified, instantiate this cursor
                class instead of the one specified in the constructor's `cursor_class`
                argument.
        """

    @abstractmethod
    def execute(
        self,
        cursor: CursorT,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        **kwargs: Any,
    ) -> None:
        """
        Execute a statement without returning results, and without transaction behavior.

        Arguments:
            cursor: Cursor -- Cursor to use for execution and fetching results.
            query_string: Query | str -- The SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
        """
        query_string = self._query_string(query_string)

        if self.execute_contextmanager:
            with self.execute_contextmanager(cursor, query_string, query_params, **kwargs) as (
                # These may be updated by `execute_contextmanager`:
                new_query_string,
                new_query_params,
            ):
                cursor.execute(new_query_string, new_query_params)
        else:
            cursor.execute(query_string, query_params)

    def select(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Optional[Type[CursorT]] = None,
        **kwargs: Any,
    ) -> Sequence[Any]:
        """
        Execute a statement, returning results, without transaction behavior.

        Arguments:
            query_string: Query | str -- The SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
            cursor_class: Type[Cursor] -- Cursor class to use for fetching results.
        """
        cursor = self.cursor(cursor_class=cursor_class)
        try:
            self.execute(cursor, query_string, query_params, **kwargs)
            results = list(cursor.fetchall())
            return results
        finally:
            cursor.close()

    def select_row(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Optional[Type[CursorT]] = None,
        **kwargs: Any,
    ) -> Optional[Any]:
        """
        Execute a statement, returning one row, without transaction behavior.

        Arguments:
            query_string: Query | str -- The SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
            cursor_class: Type[Cursor] -- Cursor class to use for fetching results.
        """
        cursor = self.cursor(cursor_class=cursor_class)
        try:
            self.execute(cursor, query_string, query_params, **kwargs)
            for result in cursor:
                return result
            else:  # pylint: disable=useless-else-on-loop
                # No rows.
                return None
        finally:
            cursor.close()

    def select_value(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a statement, returning one value, without transaction behavior.

        Arguments:
            query_string: Query | str -- The SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
        """
        result = self.select_row(
            query_string, query_params, cursor_class=self.TupleCursor, **kwargs
        )
        return result[0] if result is not None else None

    def select_iter(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Type[CursorT] = None,
        batch_size: Optional[int] = None,
        fetch_batch_size: Optional[int] = None,
        **kwargs: Any,
    ) -> Iterator:
        """
        Execute a statement, yielding one row or a batch of rows at a time, without
        transaction behavior.

        Arguments:
            query_string: Query | str -- The SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
            cursor_class: Type[Cursor] -- Cursor class to use for fetching results.
            batch_size: int (optional) -- Yield this many rows at a time. By default, or
                if `0` or `None`, yields a single row at a time (not a list). Otherwise,
                yields lists of this many rows at a time. (Note that `None` and `1` both
                yield one row at a time, but the former yields just a row object whereas the
                latter yields single-element lists.)
            fetch_batch_size: int (optional) -- Fetch this many rows at a time (default:
                same as `batch_size`, at least 1). Increase this for more efficient use of
                the network connection.
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        cursor = self.cursor(cursor_class=cursor_class)
        fetch_batch_size = fetch_batch_size or batch_size or 1
        try:
            self.execute(cursor, query_string, query_params, **kwargs)
            done = False
            buffer: List[Any] = []
            while not done:
                while len(buffer) < (batch_size or 1):
                    results = cursor.fetchmany(fetch_batch_size)
                    buffer.extend(results)
                    if len(results) < fetch_batch_size:
                        done = True
                        break
                while len(buffer) >= (batch_size or 1):
                    if batch_size:
                        yield buffer[:batch_size]
                        buffer[:batch_size] = []
                    else:
                        yield buffer.pop(0)
            if buffer:
                # Possibly there were not enough rows in the buffer to yield a whole batch.
                # This implies that `batch_size > 1`. Yield any remaining rows as a list now.
                yield buffer
        finally:
            cursor.close()

    def exists(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        **kwargs: Any,
    ) -> bool:
        """
        Returns whether the given `SELECT` statement would return any rows, without sending
        any rows over the wire.

        For example:

        >>> database_adapter.exists("SELECT * FROM mysql.user")
        True
        """
        query_string = self._query_string(query_string)
        return bool(self.select_value(f"SELECT EXISTS ({query_string})", query_params, **kwargs))

    @abstractmethod
    def found_rows(self) -> int:
        """
        Returns the number of rows the preceding `SELECT` statement would have returned
        without a `LIMIT` clause.

        Not all database systems/adapters support this.
        """

    @abstractmethod
    def last_insert_id(self) -> Any:
        """
        Returns the value of the most recently inserted primary-key/auto-increment column.

        Not all database systems/adapters support this.
        """

    # pylint: disable=undefined-variable  # <https://github.com/PyCQA/pylint/issues/3461>
    @abstractmethod
    def explain(
        self, query_string: QueryT, query_params: Optional[QueryParams] = None
    ) -> QueryPlan:
        """
        Requests a query plan for the given SQL statement and returns a structured
        representation of the plan.
        """

    @abstractmethod
    def cancel(self) -> None:
        """
        Cancels the current query. The only way to call this is from another thread.
        """


def get_adapter_class(database_type: DatabaseType) -> Type[DatabaseAdapter]:
    """Returns database-specific adapter class for given database type."""
    return DatabaseAdapter.adapter_class(database_type)
