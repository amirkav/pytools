"""
Low-level operations on PostgreSQL databases.
Defines methods to connect, set db parameters, run transactions, explain queries, etc.
This module is not meant to be used directly, but rather through the higher-level methods
defined in the `pytools.sql.sql_connect` module.
"""

import contextlib
import re
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional, Tuple, Type

import psycopg2
import psycopg2.extensions
import psycopg2.extras

import pytools.common.retry_backoff
import pytools.sql.adapters.base
from pytools.common.class_tools import cached_property
from pytools.common.file_utils import relativize_path
from pytools.common.retry_backoff import RetryAndBackoff
from pytools.common.call_stack import getcaller

from ..query import QueryT
from ..route import Route
from .base import (
    DatabaseAdapter,
    ExecuteContextManagerFactory,
    IsolationLevel,
    QueryParams,
    QueryPlan,
    QueryPlanNode,
)
from .errors import ResponseError

# Types
# =============================================================================

# <https://www.psycopg.org/docs/connection.html>
PostgreSQLConnection = psycopg2.extensions.connection

# <https://www.psycopg.org/docs/cursor.html>
PostgreSQLCursor = psycopg2.extensions.cursor
PostgreSQLTupleCursor = psycopg2.extensions.cursor

# <https://www.psycopg.org/docs/extras.html#real-dictionary-cursor>
PostgreSQLDictCursor = psycopg2.extras.RealDictCursor


# PostgreSQL retry decorator
# =============================================================================


class pg_retry(RetryAndBackoff):
    default_exceptions = (
        # <https://www.psycopg.org/docs/module.html#psycopg2.InterfaceError>
        psycopg2.InterfaceError,
        # <https://www.psycopg.org/docs/module.html#psycopg2.OperationalError>
        psycopg2.OperationalError,
        #
        # Retry only on very specific exceptions where a retry is actually likely to succeed:
        # <https://www.psycopg.org/docs/errors.html#sqlstate-exception-classes>
        #
        # `DatabaseError` subclasses:
        psycopg2.errors.ConnectionException,
        psycopg2.errors.SqlclientUnableToEstablishSqlconnection,
        psycopg2.errors.ConnectionDoesNotExist,
        psycopg2.errors.SqlserverRejectedEstablishmentOfSqlconnection,
        psycopg2.errors.ConnectionFailure,
        #
        # `OperationalError` subclasses:
        psycopg2.errors.DeadlockDetected,
    )


# PostgreSQL QueryPlan classes
# =============================================================================


@dataclass
class PostgreSQLQueryPlanNode(QueryPlanNode):
    """
    PostgreSQL query plan node.

    Attributes:
        type: str -- Type of the plan node.
        parallel_aware: bool -- Whether this node of the plan can execute in parallel to
            other nodes of the same plan.
        startup_cost: float -- Estimated cost to produce the first row of this node's
            result set.
        total_cost: float -- Estimated cost to produce this node's entire result set.
        plan_rows: int -- Estimated number of rows produced by this node.
        plan_width: int -- Estimated size (in bytes) of a row produced by this node.
        parent_relationship: str -- The node's parent relationship.
        join_type: str -- The join type used by this node (if any).
        function_name: str -- The name of the function this node represents (if any).
        alias: str -- The table alias used by this node (if any).
        inner_unique: bool -- Whether an inner join criterion is unique.
        hash_cond: str -- The node's hash condition (if any).
        nodes: list -- The sub-nodes of this node.
    """

    type: str
    parallel_aware: bool
    startup_cost: float
    total_cost: float
    plan_rows: int
    plan_width: int

    parent_relationship: Optional[str] = None
    join_type: Optional[str] = None
    function_name: Optional[str] = None
    alias: Optional[str] = None
    inner_unique: Optional[bool] = None
    hash_cond: Optional[str] = None

    nodes: list["PostgreSQLQueryPlanNode"] = field(default_factory=list)


@dataclass
class PostgreSQLQueryPlan(QueryPlan):
    """
    PostgreSQL query plan.

    See <https://www.postgresql.org/docs/current/using-explain.html> for details.
    """

    Node = PostgreSQLQueryPlanNode

    node: PostgreSQLQueryPlanNode


# PostgreSQL adapter class
# =============================================================================


class PostgreSQLAdapter(
    # <https://github.com/python/mypy/issues/9560>
    DatabaseAdapter[PostgreSQLConnection, PostgreSQLCursor],  # type: ignore
):
    """PostgreSQL database adapter class."""

    TupleCursor: Type[PostgreSQLCursor] = PostgreSQLTupleCursor
    DictCursor: Type[PostgreSQLCursor] = PostgreSQLDictCursor
    QueryPlan: Type[PostgreSQLQueryPlan] = PostgreSQLQueryPlan

    def __init__(
        self,
        route: Route,
        *,
        autocommit: bool = False,
        cursor_class: Type[PostgreSQLCursor] = PostgreSQLCursor,
        execute_contextmanager: Optional[ExecuteContextManagerFactory] = None,
    ) -> None:
        self.cursor_class: Type[PostgreSQLCursor]
        super().__init__(
            route,
            autocommit=autocommit,
            cursor_class=cursor_class,
            execute_contextmanager=execute_contextmanager,
        )

        self._server_version: Optional[Tuple[int, ...]] = None

    @pg_retry()
    def connect(self) -> PostgreSQLConnection:
        # Convey caller information in PostgreSQL's `application_name` parameter:
        # <https://www.postgresql.org/docs/12/runtime-config-logging.html#GUC-APPLICATION-NAME>
        # This makes connections identifiable in the `pg_stat_activity` view.
        # <https://www.postgresql.org/docs/12/monitoring-stats.html#PG-STAT-ACTIVITY-VIEW>
        caller = getcaller(
            # Ignore frames from within this module:
            ignore_filenames=[__file__],
            # Ignore frames from within these other modules:
            ignore_modules=[
                contextlib,
                pytools.common.dynamic_namespace,
                pytools.common.retry_backoff,
                pytools.sql.adapters.base,
            ],
        )
        caller_filename = relativize_path(caller.filename, sys.path)
        application_name = f"{caller_filename}:{caller.lineno}:{caller.function}"
        if len(application_name) > 63:
            # Shorten to no more than 63 characters, or PostgreSQL will do it and emit a warning:
            application_name = f"{application_name[:60]}..."

        with self.route.physical_route() as route:
            kwargs: dict[str, Any] = {
                "host": route.host,
                "port": route.port,
                "user": route.user,
                "password": route.password,
                "dbname": route.database,
                "client_encoding": "utf8",
                "application_name": application_name,
                **route.connect_args,
            }
            self._connection = psycopg2.connect(**kwargs)
            self._connection.autocommit = self.autocommit
        return self._connection

    @property
    def is_open(self) -> bool:
        if self._connection is None:
            return False
        return self.connection.closed == 0

    def quote_ident(self, name: str) -> str:
        """Returns a quoted representation of the given identifier."""
        return psycopg2.extensions.quote_ident(name, self.connection)

    def connection_count_for_database(self, database: Optional[str] = None) -> int:
        if database:
            return self.select_value(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = %s", (database,)
            )
        return self.select_value("SELECT count(*) FROM pg_stat_activity")

    def _getvar(self, path: list[str]) -> Any:
        return self.select_value(f"SHOW {'.'.join(path)}")

    def _setvar(self, path: list[str], value: Any) -> None:
        cursor = self.connection.cursor()
        cursor.execute(f"SET {'.'.join(path)} = %s", (value,))

    def getvar(self, name: str) -> Any:  # pylint: disable=useless-super-delegation
        """
        Get the value of the given database run-time parameter.
        This is equivalent to `SHOW name`.

        Arguments:
            name: str -- Name of the database run-time parameter to get.

        Returns:
            Value of the database run-time parameter.
        """
        return super().getvar(name)

    def setvar(self, name: str, value: Any) -> None:  # pylint: disable=useless-super-delegation
        """
        Set the value of the given database run-time parameter.
        This is equivalent to `SET name = value`.

        Arguments:
            name: str -- Name of the database run-time parameter to set.
            value: Any -- Value to assign to the database run-time parameter.
        """
        return super().setvar(name, value)

    @cached_property
    def server_version(self) -> Optional[Tuple[int, ...]]:
        """
        Returns the server version as a tuple of integers for easy comparison in Python
        code.

        For example:

        >>> if database_adapter.server_version >= (12, 3):
        ...     # use new feature in PostgreSQL 12.3
        """
        server_version = self.select_value("SHOW server_version")
        if server_version is None:
            return None
        match = re.match(r"^\d+(?:\.\d+)*", server_version)
        if match is None:
            raise ResponseError(
                f"Unable to parse server version from `server_version` variable: {server_version}"
            )
        self._server_version = tuple(int(part) for part in match[0].split("."))
        return self._server_version

    @cached_property
    def identifier_quote_char(self) -> str:
        """Returns the quote character recognized by the database."""
        return '"'

    @property
    def connection_id(self) -> int:
        """
        Returns the unique ID of the database connection.

        See <https://www.postgresql.org/docs/13/functions-info.html#id-1.5.8.32.4.2.2.11.1.1.1>.
        """
        return self.select_value("SELECT pg_backend_pid()")

    @property
    def transaction_id(self) -> Optional[int]:
        """
        Returns the unique ID of the current transaction, if any.

        See <https://www.postgresql.org/docs/current/functions-info.html#FUNCTIONS-PG-SNAPSHOT>.
        """
        if self.server_version and self.server_version >= (10, 0):
            transaction_id = self.select_value("SELECT txid_current_if_assigned()")
        else:
            transaction_id = self.select_value("SELECT txid_current()")
        return int(transaction_id) if transaction_id is not None else None

    @property
    def transaction_isolation(self) -> IsolationLevel:
        transaction_isolation_str = self.vars.transaction_isolation.upper().replace(" ", "_")
        return IsolationLevel[transaction_isolation_str]

    @contextmanager
    @pg_retry()
    def transaction(self, isolation_level: Optional[IsolationLevel] = None) -> Iterator:
        variables = {}
        if isolation_level:
            if self._transaction_level > 0:
                raise ValueError(
                    f"isolation_level={isolation_level} specified in nested transaction; "
                    f"allowed only in top-level transaction"
                )
            transaction_isolation_str = isolation_level.name.lower().replace("_", " ")
            variables["transaction_isolation"] = transaction_isolation_str

        with self.setvars(variables):
            try:
                self._transaction_level += 1
                if self._transaction_level == 1:
                    with self.connection:
                        yield self
                else:
                    yield self
            finally:
                self._transaction_level -= 1

    def cursor(self, cursor_class: Optional[Type[PostgreSQLCursor]] = None) -> PostgreSQLCursor:
        return self.connection.cursor(
            cursor_factory=self._concrete_cursor_class(cursor_class or self.cursor_class)
        )

    @pg_retry()
    def execute(
        self,
        cursor: PostgreSQLCursor,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        **kwargs: Any,
    ) -> None:
        super().execute(cursor, query_string, query_params, **kwargs)

    def found_rows(self) -> int:
        """Not supported by PostgreSQL."""
        # MySQL's `found_rows()` ignores `LIMIT` and calculates the theoretical total number of
        # results. PostgreSQL has no direct equivalent of this.
        raise NotImplementedError(
            "MySQL's `found_rows()` ignores LIMIT, but PostgreSQL has no direct equivalent"
        )

    def last_insert_id(self) -> Any:
        """Not supported by PostgreSQL; use a `RETURNING` clause in `INSERT` statement instead."""
        # PostgreSQL has no direct equivalent of MySQL's `last_insert_id` function; instead use a
        # `RETURNING` clause in `INSERT` statement.
        # <https://www.postgresql.org/docs/current/dml-returning.html>
        raise NotImplementedError("Use `RETURNING` clause in `INSERT` statement instead")

    def explain(
        self, query_string: QueryT, query_params: Optional[QueryParams] = None
    ) -> PostgreSQLQueryPlan:
        """
        Requests a query plan for the given SQL statement and returns a structured
        representation of the plan. See `PostgreSQLQueryPlan` for details.
        """
        query_string = self._query_string(query_string)
        result = self.select_value(f"EXPLAIN (FORMAT JSON) {query_string}", query_params)
        root_node = result[0]["Plan"]

        def parse_node(node_dict: dict[str, Any]) -> PostgreSQLQueryPlanNode:
            def normalize_key(key: str) -> str:
                key = key.lower().replace(" ", "_")
                if key == "node_type":
                    key = "type"
                return key

            node_dict = {normalize_key(key): value for key, value in node_dict.items()}
            if "plans" in node_dict:
                node_dict["plans"] = [
                    parse_node(subnode_dict) for subnode_dict in node_dict["plans"]
                ]
            return PostgreSQLQueryPlanNode(**node_dict)

        query_plan = PostgreSQLQueryPlan(node=parse_node(root_node))
        return query_plan

    def cancel(self) -> None:
        """
        Cancels the current query. The only way to call this is from another thread.

        If successful, this will raise a `QueryCanceledError` exception in the thread that issued
        the query.

        See <https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.QueryCanceledError>.
        """
        # <https://www.psycopg.org/docs/connection.html#connection.cancel>
        self.connection.cancel()
