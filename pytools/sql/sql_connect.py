"""
High-level methods to run queries against a PostgreSQL database.
It relies on the low-level methods in `pytools.sql.adapters.postgresql`.
Does not include any logic specific to data or database structure.
"""

import re
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Iterator,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import jinja2

from pytools.aws.boto3_session_generator import Boto3Session
from pytools.aws.ssm_connect import SsmConnect
from pytools.common.doc_utils import documented_by
from pytools.common.dynamic_namespace import DynamicNamespace
from pytools.common.logger import Logger

from .adapters import PostgreSQLAdapter
from .adapters.base import (
    DatabaseAdapter,
    IsolationLevel,
    QueryParams,
    QueryPlan,
    QueryPlanNode,
)
from .pep249 import Connection, Cursor, DictCursor, TupleCursor
from .query import Query, QueryT
from .route import Route

# if TYPE_CHECKING:
from pytools.app.configs import Configs

__all__ = [
    "SQLConnect",
    "IsolationLevel",
    "QueryPlan",
    "QueryPlanNode",
]


# SQLConnect class
###############################################################################

_S = TypeVar("_S", bound="SQLConnect")


class SQLConnect:
    """
    Provides access to a SQL database (MySQL or PostgreSQL).

    This class has the following capabilities:
    - Establish DB connection
    - Act as a context manager, automatically opening and closing DB connection
    - Read SQL statement from a template file
    - Execute arbitrary SQL statements
    - Execute INSERT/UPDATE statements and return stats on rows affected
    - Execute atomic UPSERT (INSERT+DELETE) statements and return stats on rows
      affected
    - Time SQL statement execution and notify if execution timing exceeds a threshold
    - Automatic retry with exponential back-off (see below)
    - Access to primary key of last inserted row

    RETRY BEHAVIOR: Certain database errors are automatically retried (generally only those
    that are worth retrying, such as connection errors or certain resource errors)
    with exponential back-off, using `pytools.common.retry_backoff.RetryAndBackoff`.
    """

    def __init__(
        self,
        route: Route,
        *,
        autocommit: bool = False,
        cursor_class: Optional[Type[Cursor]] = None,
    ) -> None:
        """
        Create a `SQLConnect` object based on the route given.

        Arguments:
            route -- `Route` object with connection/authentication information.
            cursor_class -- Cursor class to instantiate.
                For PostgreSQL, it must be a subclass of `psycopg2.extensions.cursor` (default);
                cursor classes supported out of the box are `psycopg2.extensions.cursor`,
                `psycopg2.extras.DictCursor`, `psycopg2.extras.RealDictCursor`,
                `psycopg2.extras.NamedTupleCursor`.
        """
        self.adapter = PostgreSQLAdapter(
            route=route,
            autocommit=autocommit,
            cursor_class=cursor_class or TupleCursor,
            execute_contextmanager=self._execute_contextmanager,
        )

        self._logger = Logger(__name__)
        self._transaction_level: int = 0

    @classmethod
    def from_config(
        cls,
        configs: Configs,
        *,
        bastion_host: Optional[str] = None,
        boto3_session: Optional[Boto3Session] = None,
        ssm_connect: Optional[SsmConnect] = None,
        use_writer: bool = False,
        use_master: bool = False,
        autocommit: bool = False,
        cursor_class: Optional[Type[Cursor]] = None,
    ) -> "SQLConnect":
        """
        Create a `SQLConnect` object and route to the appropriate endpoint based on the arguments
        given.

        Arguments:
            config -- `pytools.app.config.Config` object that provides AWS region, database endpoints,
                database user names, and the database name.
            boto3_session -- (optional) `boto3.session.Boto3Session` object that provides access to
                AWS.
            ssm_connect -- (optional) `pytools.aws.ssm_connect.SsmConnect` object that provides database
                passwords and CA SSL certificates.
            use_writer -- (optional) Boolean indicating whether write access is required.
                (default: False)
            use_master -- (optional) Boolean indicating whether master-user access is required.
                If `use_master` is specified, `use_writer` is ignored. (default: False)
            autocommit -- (optional) Boolean indicating whether to use autocommit mode.
        """
        route = Route.from_config(
            config=configs,
            bastion_host=bastion_host,
            boto3_session=boto3_session,
            ssm_connect=ssm_connect,
            use_writer=use_writer,
            use_master=use_master,
        )
        return cls(route=route, autocommit=autocommit, cursor_class=cursor_class)

    # Properties
    # =========================================================================

    @property
    def route(self) -> Route:
        """Returns `Route` object used."""
        return self.adapter.route

    @property
    def logger(self) -> Logger:
        """Returns `tools.logger.Logger` object used."""
        return self._logger

    # Methods
    # =========================================================================

    @staticmethod
    def get_query_string(query_file: Path) -> str:
        """Reads a formatted SQL statement from `query_file` and returns an unwrapped string."""
        assert query_file.exists(), f"Query file path not valid: {query_file.as_posix()}"
        return query_file.read_text()

    def read_query(
        self, query_path: Path, query_fragments: Optional[dict[str, Any]] = None
    ) -> Optional[QueryT]:
        """
        Reads a Jinja2-format template file with the given path and renders the template to a
        generic `Query` object using the given query fragments.

        Arguments:
            query_path: Path -- Path of query template.
            query_fragments: dict -- Mapping of template placeholder names to values.
        """
        query_fragments = query_fragments.copy() if query_fragments else {}
        template = jinja2.Template(query_path.read_text())
        query_string = template.render(query_fragments).strip()
        return Query(query_string) if query_string else None

    @documented_by(DatabaseAdapter.vars)  # type: ignore
    @property
    def vars(self) -> DynamicNamespace:
        return self.adapter.vars

    @documented_by(DatabaseAdapter.setvars)
    @contextmanager
    def setvars(self: _S, variables: Optional[dict] = None, **kwvars: Any) -> Iterator[_S]:
        with self.adapter.setvars(variables, **kwvars):
            yield self

    @contextmanager
    def open(self: _S, close: bool = True) -> Iterator[_S]:
        """
        Context manager that automatically opens a database connection before entering the context
        and optionally closes it after exiting the context.

        Arguments:
            close: `bool` controlling whether to close connection when exiting context
                (default: True).
        """
        self.connect()
        try:
            yield self
        except Exception as e:
            self.logger.exception(e)
            raise
        finally:
            if close:
                self.adapter.close()

    @documented_by(DatabaseAdapter.transaction)
    @contextmanager
    def transaction(self: _S, isolation_level: Optional[IsolationLevel] = None) -> Iterator[_S]:
        with self.adapter.transaction(isolation_level=isolation_level):
            yield self

    @documented_by(DatabaseAdapter.connect)
    def connect(self) -> Connection:
        return self.adapter.connect()

    @documented_by(DatabaseAdapter.is_open)  # type: ignore
    @property
    def is_open(self) -> bool:
        return self.adapter.is_open

    @documented_by(DatabaseAdapter.connection_id)  # type: ignore
    @property
    def connection_id(self) -> Optional[int]:
        return self.adapter.connection_id

    @documented_by(DatabaseAdapter.transaction_id)  # type: ignore
    @property
    def transaction_id(self) -> Optional[int]:
        return self.adapter.transaction_id

    @documented_by(DatabaseAdapter.transaction_isolation)  # type: ignore
    @property
    def transaction_isolation(self) -> IsolationLevel:
        return self.adapter.transaction_isolation

    @documented_by(DatabaseAdapter.transaction_level)  # type: ignore
    @property
    def transaction_level(self) -> int:
        return self.adapter.transaction_level

    # Match any sequence of word characters enclosed in a pair of identifier quotes (either double
    # quotes or backticks):
    _SQL_QUOTED_IDENTIFIER_PATTERN = re.compile(r"([\"`])(\w+)\1")

    @contextmanager
    def _execute_contextmanager(
        self,
        cursor: Cursor,
        query_string: Query,
        query_params: Optional[QueryParams] = None,
    ) -> Iterator:
        """
        Wraps the execution of an SQL statement in SQLConnect-specific behavior:
        - Log the rendered version of the statement using the configured logger.

        Arguments:
            cursor -- `Cursor` object on which the provided SQL statement is being executed.
            query_string -- string holding the SQL query, possibly with `%s` or `%(param)s` style
                placeholders in conjunction with query parameters.
            query_params -- dictionary or tuple with query parameters.
        """
        query_string = self._SQL_QUOTED_IDENTIFIER_PATTERN.sub(
            lambda match: self.adapter.quote_ident(match[2]),
            query_string,
        )

        rendered_query_string = cursor.mogrify(query_string, query_params)
        if isinstance(rendered_query_string, bytes):
            rendered_query_string = rendered_query_string.decode()
        self._logger.debug(f"Executing query: {rendered_query_string}")

        yield query_string, query_params

    def execute(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Type[Cursor] = None,
    ) -> None:
        """
        Executes a statement without returning results, and without transaction behavior.

        Arguments:
            query_string: Query | str -- String containing the SQL query to execute.
            query_params: tuple | dict (optional) -- Query parameters.
            cursor_class: Type[Cursor] -- Cursor class to use for fetching results.
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        cursor = self.adapter.cursor(cursor_class=cursor_class)
        try:
            self.adapter.execute(cursor, query_string, query_params)
        finally:
            cursor.close()

    @documented_by(DatabaseAdapter.select)
    def select(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Type[Cursor] = None,
    ) -> Sequence[Any]:
        """
        Arguments:
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        return self.adapter.select(query_string, query_params, cursor_class=cursor_class)

    @documented_by(DatabaseAdapter.select_row)
    def select_row(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Type[Cursor] = None,
    ) -> Any:
        """
        Arguments:
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        return self.adapter.select_row(query_string, query_params, cursor_class=cursor_class)

    @documented_by(DatabaseAdapter.select_value)
    def select_value(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
    ) -> Any:
        """
        Arguments:
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        return self.adapter.select_value(query_string, query_params)

    @documented_by(DatabaseAdapter.select_iter)
    def select_iter(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        cursor_class: Type[Cursor] = None,
        batch_size: Optional[int] = None,
        fetch_batch_size: Optional[int] = None,
    ) -> Iterator:
        """
        Arguments:
            slow_threshold: float (optional) -- After how many seconds a query is
                considered slow.
        """
        yield from self.adapter.select_iter(
            query_string,
            query_params,
            cursor_class=cursor_class,
            batch_size=batch_size,
            fetch_batch_size=fetch_batch_size,
        )

    def get_dict_query_results(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
    ) -> Sequence[dict[str, Any]]:
        """
        Returns the query results as a list, with each entry encoded as a dictionary.
        The keys are the lower-case column names from the query results.
        """
        results = self.adapter.select(
            query_string,
            query_params,
            cursor_class=DictCursor,
        )
        results = [{k: v for k, v in row.items()} for row in results]
        return results

    @documented_by(DatabaseAdapter.exists)
    def exists(
        self,
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
    ) -> bool:
        return self.adapter.exists(query_string, query_params)

    def get_row_count(self) -> Optional[int]:
        """Returns count of rows found (disregarding LIMIT) for the last-executed query."""
        return self.adapter.found_rows()

    def last_row_id(self) -> Any:
        """
        Returns the id of the last row inserted for this connection.
        See: https://dev.mysql.com/doc/refman/5.7/en/getting-unique-id.html
        Each client will receive the last inserted ID for the last statement that client executed.
        """
        return self.adapter.last_insert_id()

    def explain(
        self, query_string: QueryT, query_params: Optional[QueryParams] = None
    ) -> QueryPlan:
        """
        Explains a `SELECT` query with the specified query parameters and returns the query plan
        nodes.

        Arguments:
            query_string: Query | str -- SQL query text with placeholders for query parameters.
            query_params: dict -- Map query parameters to their values.
        """
        return self.adapter.explain(query_string, query_params)

    def cancel(self) -> None:
        """
        This is currently supported only on PostgreSQL, not on MySQL.

        Cancels the current query. The only way to call this is from another thread.

        On PostgreSQL, if successful, this will raise a `QueryCanceledError` exception in the thread
        that issued the query.

        See <https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.QueryCanceledError>.
        """
        self.adapter.cancel()

    # High-level utility methods
    # =========================================================================

    def execute_insert_update_query(
        self,
        table_name: Optional[str],
        query_string: QueryT,
        query_params: Optional[QueryParams] = None,
        *,
        full_stats: bool = False,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> dict[str, Any]:
        """
        Inserts data into a table using the provided query string. This query string
        may include multiple row literals to be inserted, and it may include an
        `ON DUPLICATE KEY …` (MySQL) or `ON CONFLICT …` (PostgreSQL) clause to implement
        UPSERT behavior.

        The operation is implicitly performed inside a transaction. It will run fine
        inside an explicitly opened transaction in case atomicity with other operations
        is required.

        Returns a `dict` representing an "UPSERT report" with information about the
        effects of the operation.

        Arguments:
            table_name: str -- Name of the table being inserted into. If full stats are
                requested (`full_stats=True`) this is used to determine the row count
                before and after the operation.
            query_string: Query | str -- The SQL query to execute.
            query_params: dict (optional) -- Mapping of query parameters to values.
            full_stats: bool (optional) -- Whether to calculate "before" and "after" row
                counts from the provided table (default: False). This is expensive.
            isolation_level: IsolationLevel (optional) -- Execute the transaction with
                the given isolation level. See the `IsolationLevel` enum class for the
                supported isolation levels.

        Returns:
            Dict representing an "UPSERT report" with information about the effects of
            the operation.
        """
        inserted_rows, updated_rows, duplicate_rows, deleted_rows = 0, 0, 0, 0
        affected_rows = 0
        cnt_query = f"SELECT COUNT(*) FROM {table_name}"

        with self.adapter.transaction(isolation_level=isolation_level):
            cursor = self.adapter.cursor()
            try:
                before_cnt = 0
                if full_stats:
                    # get current num rows before running the query
                    before_cnt = self.adapter.select_value(cnt_query)

                # run the main query
                self.adapter.execute(cursor, query_string, query_params)
                affected_rows = cursor.rowcount  # updated rows count as 2, inserted rows count as 1

                if full_stats:
                    # again, get current num rows after running the query
                    after_cnt = self.adapter.select_value(cnt_query)
                    if after_cnt >= before_cnt:
                        inserted_rows = after_cnt - before_cnt
                        updated_rows = (affected_rows - inserted_rows) // 2
                        duplicate_rows = affected_rows - inserted_rows
                    else:
                        deleted_rows = before_cnt - after_cnt
                        updated_rows = (affected_rows - deleted_rows) // 2
                        duplicate_rows = affected_rows - deleted_rows
            finally:
                cursor.close()

        upsert_report = {
            "table_name": table_name,
            "success": 1,
            "affected_rows": int(affected_rows),
            "inserted_rows": int(inserted_rows),
            "deleted_rows": int(deleted_rows),
            "duplicate_rows": int(duplicate_rows),
            "updated_rows": int(updated_rows),
        }

        self.logger.json(upsert_report, level=Logger.INFO)
        return upsert_report

    def execute_delete_insert_query(
        self,
        table_name: str,
        insert_query_string: QueryT,
        insert_query_params: QueryParams,
        delete_query_string: QueryT,
        delete_query_params: QueryParams,
        *,
        full_stats: bool = False,
        isolation_level: Optional[IsolationLevel] = None,
    ) -> dict[str, Any]:
        """
        Executes and commits a pair of insert and delete queries as one ACID operation
        and reports back the results.
        If one of the operations fail, it rolls back the entire transaction.

        Atomically replaces a set of data in a table by first deleting certain existing
        rows using the given `delete` query string and then inserting new rows using the
        given `insert` query string. The `insert` query string may include multiple row
        literals to be inserted.

        The operation is implicitly performed inside a transaction. It will run fine
        inside an explicitly opened transaction in case atomicity with other operations
        is required.

        Returns a `dict` representing an "UPSERT report" with information about the
        effects of the operation.

        Arguments:
            table_name: str -- Name of the table being inserted into. If full stats are
                requested (`full_stats=True`) this is used to determine the row count
                before and after the operation.
            insert_query_string: Query | str -- SQL query inserting data into the table.
            insert_query_params: dict -- Mapping of insert query parameters to values.
            delete_query_string: Query | str -- SQL query deleting data from the table.
            delete_query_params: dict -- Mapping of delete query parameters to values.
            full_stats: bool (optional) -- Whether to calculate "before" and "after" row
                counts from the provided table (default: False). This is expensive.
            isolation_level: IsolationLevel (optional) -- Execute the transaction with
                the given isolation level. See the `IsolationLevel` enum class for the
                supported isolation levels.

        Returns:
            dict representing an "UPSERT report" with information about the effects of
            the operation.
        """
        inserted_rows, updated_rows, deleted_rows = 0, 0, 0
        insert_affected_rows, delete_affected_rows = 0, 0
        cnt_query = f"SELECT COUNT(*) FROM {table_name}"

        with self.adapter.transaction(isolation_level=isolation_level):
            cursor = self.adapter.cursor()
            try:
                # get current num rows before running the query
                before_cnt = 0
                if full_stats:
                    before_cnt = self.adapter.select_value(cnt_query)

                # execute the delete query
                self.adapter.execute(cursor, delete_query_string, delete_query_params)
                # updated rows count as 2, inserted rows count as 1
                delete_affected_rows = cursor.rowcount

                # execute the insert query
                self.adapter.execute(cursor, insert_query_string, insert_query_params)
                # updated rows count as 2, inserted rows count as 1
                insert_affected_rows = cursor.rowcount

                # again, get current num rows after running the query
                if full_stats:
                    after_cnt = self.adapter.select_value(cnt_query)
                    if after_cnt > before_cnt:
                        inserted_rows = after_cnt - before_cnt
                        deleted_rows = 0
                        updated_rows = insert_affected_rows - delete_affected_rows
                    elif after_cnt < before_cnt:
                        deleted_rows = before_cnt - after_cnt
                        inserted_rows = 0
                        updated_rows = delete_affected_rows - insert_affected_rows
                    else:
                        inserted_rows = 0
                        deleted_rows = 0
                        updated_rows = insert_affected_rows
            finally:
                cursor.close()

        upsert_report = {
            "table_name": table_name,
            "success": 1,
            "inserted_rows": int(inserted_rows),
            "insert_affected_rows": int(insert_affected_rows),
            "deleted_rows": int(deleted_rows),
            "delete_affected_rows": int(delete_affected_rows),
            "updated_rows": int(updated_rows),
        }

        self.logger.json(upsert_report, level=Logger.INFO)
        return upsert_report

    def yield_fetchmany_query(
        self, query_string: QueryT, query_params: Optional[QueryParams] = None, size: int = 100
    ) -> Iterator[list[dict[str, Any]]]:
        """Executes a user-defined fetchmany query. Returns a generator for additional results."""
        cursor = self.adapter.cursor(DictCursor)
        try:
            self.adapter.execute(cursor, query_string, query_params)
            results = cursor.fetchmany(size=size)
            while results:
                yield results
                results = cursor.fetchmany(size=size)
        finally:
            cursor.close()
