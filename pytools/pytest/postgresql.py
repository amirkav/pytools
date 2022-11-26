"""
Utility functions for writing PostgreSQL-enabled tests in `tools` and other packages.
"""

import os
from typing import Any, Iterator

from custody_py_tools.sql import DatabaseType, Route

# Temporarily avoid `pytest` dependency to allow `pylint` to run without installing dev deps:
# import pytest


# isort butchers this import because of the aliased import, hence we tell it to lay off:
from custody_py_tools.postgresql_local import (  # pylint: disable=unused-import; isort: skip
    DEFAULT_POSTGRESQL_CONTAINER,
    DEFAULT_POSTGRESQL_VERSION,
    DEFAULT_POSTGRESQL_HOST,
    DEFAULT_ROUTE as LOCAL_DEFAULT_ROUTE,
    postgresql_via_env,
    postgresql_via_docker,
)


pytest_ArgParser = Any
pytest_FixtureRequest = Any


DEFAULT_ROUTE = LOCAL_DEFAULT_ROUTE.replace(database="test_db")


def register_cli_options_postgresql(parser: pytest_ArgParser) -> None:
    """
    Add pytest `--postgresql-...` command-line options for configuring PostgreSQL container.
    """
    parser.addoption(
        "--postgresql-container",
        default=DEFAULT_POSTGRESQL_CONTAINER,
        help="Name of PostgreSQL container (default: %(default)s).",
        metavar="CONTAINER",
    )
    parser.addoption(
        "--postgresql-version",
        default=DEFAULT_POSTGRESQL_VERSION,
        help="Version of PostgreSQL to use (default: %(default)s).",
        metavar="VERSION",
    )
    parser.addoption(
        "--postgresql-port",
        default=DEFAULT_ROUTE.port,
        type=int,
        help="TCP port on which to bind PostgreSQL (default: %(default)s).",
        metavar="PORT",
    )
    parser.addoption(
        "--postgresql-user",
        default=DEFAULT_ROUTE.user,
        help="Name of PostgreSQL user to connect as (default: %(default)s).",
        metavar="USER",
    )
    parser.addoption(
        "--postgresql-pwd",
        default=DEFAULT_ROUTE.password,
        help="Password of PostgreSQL user to connect as (default: %(default)s).",
        metavar="PASSWORD",
    )
    parser.addoption(
        "--postgresql-database",
        default=DEFAULT_ROUTE.database,
        help="PostgreSQL database to connect to (default: %(default)s).",
        metavar="DATABASE",
    )
    parser.addoption(
        "--postgresql-drop-db",
        action="store_true",
        help="Drop PostgreSQL database after completion.",
    )


def _pytest_getoption(request: pytest_FixtureRequest, option: str) -> Any:
    try:
        return request.config.getoption(option)
    except ValueError as e:
        if e.args[0].startswith("no option named "):
            raise ValueError(f"{e}; did you register pytest CLI options in conftest.py?") from None
        raise


# Temporarily avoid `pytest` dependency to allow `pylint` to run without installing dev deps:
# @pytest.fixture(scope="session")
def postgresql_route(request: pytest_FixtureRequest) -> Iterator[Route]:
    if "PGHOST" in os.environ:
        # Use pre-provisioned PostgreSQL.
        yield from postgresql_via_env()

    else:
        # Use Docker container under our control.
        route = Route(
            database_type=DatabaseType.POSTGRESQL,
            host=DEFAULT_POSTGRESQL_HOST,
            port=_pytest_getoption(request, "postgresql_port"),
            user=_pytest_getoption(request, "postgresql_user"),
            password=_pytest_getoption(request, "postgresql_pwd"),
            database=_pytest_getoption(request, "postgresql_database"),
            bastion_host=None,
        )
        yield from postgresql_via_docker(
            route,
            postgresql_container=_pytest_getoption(request, "postgresql_container"),
            postgresql_version=_pytest_getoption(request, "postgresql_version"),
            postgresql_drop_db=_pytest_getoption(request, "postgresql_drop_db"),
            rm_container=_pytest_getoption(request, "rm_containers"),
        )
