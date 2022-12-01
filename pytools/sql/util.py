import time
from dataclasses import dataclass
from typing import List, Optional

import psycopg2
import pymysql

from pytools.common.logger import Logger

from .adapters import DictCursor
from .query import Query
from .route import Route


class NotReadyError(Exception):
    pass


@dataclass
class DatabaseIndex:
    name: str
    table_schema: Optional[str]
    table_name: str
    unique: bool
    column_names: List[str]


class Util:
    def __new__(cls) -> "Util":
        raise NotImplementedError("{cls} acts as a namespace only")

    @staticmethod
    def wait_postgresql_ready(route: Route, timeout: float = 30.0, interval: int = 1) -> None:
        """
        Check for PostgreSQL availability, and wait up to `timeout` seconds for it to become ready.
        Retry no more frequently than every `interval` seconds.

        Arguments:
            route -- `Route` object with connection/authentication information.
            timeout -- (optional) Wait for up to this many seconds for PostgreSQL to become ready.
                (default: 30.0)
            interval -- (optional) Retry no more frequently than this many seconds. (default: 1)
        Raises:
            `NotReadyError` if PostgreSQL is not ready after `timeout` seconds.
        """
        # pylint: disable=redefined-outer-name
        logger = Logger(__name__)
        logger.debug(f"Waiting up to {timeout:.3} seconds for PostgreSQL to become ready ...")

        start_time = time.monotonic()
        connect_timeout = min(interval, 5)

        while time.monotonic() < start_time + timeout:
            try:
                psycopg2.connect(
                    host=route.host,
                    port=route.port,
                    user=route.user,
                    password=route.password,
                    dbname="postgres",
                    connect_timeout=connect_timeout,
                )
                return
            except psycopg2.OperationalError:
                # Connection failed.
                pass
            # Don't handle any other pymysql exceptions.
            time.sleep(interval)

        raise NotReadyError(f"PostgreSQL not ready after {timeout:.3} seconds")

    @staticmethod
    def indexes_for_table(
        sql_connect, table_name: str, table_schema: Optional[str] = None
    ) -> List[DatabaseIndex]:
        with sql_connect.transaction():
            validate_results_parity = {}
            if not table_schema:
                table_schema = sql_connect.route.database
            raw_indexes = sql_connect.select(
                query_string="""
                        SELECT
                            tc.relname AS table_name,
                            ic.relname AS index_name,
                            i.indisunique AS unique,
                            array_agg(a.attname ORDER BY a.attname) AS column_names
                        FROM
                            pg_class AS tc
                            INNER JOIN pg_index AS i
                                ON tc.oid = i.indrelid
                            INNER JOIN pg_class AS ic
                                ON i.indexrelid = ic.oid
                            INNER JOIN pg_attribute AS a
                                ON (
                                    tc.oid = a.attrelid and
                                    a.attnum = ANY(i.indkey)
                                )
                        WHERE
                            tc.relname = %(table_name)s
                        GROUP BY 1, 2, 3
                        ORDER BY 1, 2, 3
                    """,
                query_params={"table_schema": table_schema, "table_name": table_name},
                cursor_class=DictCursor,
                **validate_results_parity,
            )

        indexes = [
            DatabaseIndex(
                name=raw_index["index_name"],
                table_schema=table_schema,
                table_name=raw_index["table_name"],
                unique=raw_index["unique"],
                column_names=raw_index["column_names"],
            )
            for raw_index in raw_indexes
        ]
        return indexes

    @staticmethod
    def unique_index_for_table(
        sql_connect, table_name: str, table_schema: Optional[str] = None
    ) -> Optional[DatabaseIndex]:
        unique_indexes = [
            index
            for index in Util.indexes_for_table(sql_connect, table_name, table_schema)
            if index.unique
            and not (
                index.name == "PRIMARY"  # MySQL
                or index.name.endswith("_pkey")  # PostgreSQL
                or index.name.endswith("_oid_index")  # PostgreSQL
            )
        ]
        if len(unique_indexes) > 1:
            raise ValueError(f'"{table_name}" has more than one unique index')
        if len(unique_indexes) == 0:
            return None
        return unique_indexes[0]
