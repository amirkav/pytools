# Utility functions for running and managing PostgreSQL Docker containers.

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from docker.models.containers import Container as DockerContainer

from pytools.common.docker_utils import get_docker_client, get_docker_container
from pytools.sql import Route, SQLConnect
from pytools.sql import Util as SQLUtil

DEFAULT_POSTGRESQL_CONTAINER = "postgresql-local"
DEFAULT_POSTGRESQL_VERSION = "12.3"
DEFAULT_POSTGRESQL_HOST = "127.0.0.1"

DEFAULT_ROUTE = Route(
    database_type="postgresql",
    host=DEFAULT_POSTGRESQL_HOST,
    port=54320,
    user="postgres",
    password="nopasswd",
    database="postgresql_local",
    bastion_host=None,
)


def postgresql_via_env() -> Iterator[Route]:
    # Environment variables per <https://www.postgresql.org/docs/current/libpq-envars.html>.

    for required_var in ("PGUSER", "PGPASSWORD"):
        if not os.getenv(required_var):
            raise ValueError(f"Found PGHOST environment variable, but missing {required_var}")

    ssl = os.getenv("PGSSLMODE", "disable").lower() in (
        "prefer",
        "require",
        "verify-ca",
        "verify-full",
    )
    route = Route(
        host=os.environ["PGHOST"],
        port=int(os.getenv("PGPORT", str(DEFAULT_ROUTE.port))),
        user=os.environ["PGUSER"],
        password=os.environ["PGPASSWORD"],
        database=os.getenv("PGDATABASE", DEFAULT_ROUTE.database),
        ssl=ssl,
    )
    yield route


def _init_postgresql_container(_route: Route, _container: DockerContainer) -> None:
    # We use the built-in `postgres` superuser as the master user, so no privileges need to be
    # granted here.
    pass


def _init_postgresql_database(route: Route) -> None:
    nodb_route = route.replace(database=None)
    nodb_sql_connect = SQLConnect(nodb_route, autocommit=True)
    try:
        nodb_sql_connect.execute(f"CREATE DATABASE {route.database}")
    except psycopg2.errors.DuplicateDatabase:
        pass
    sql_connect = SQLConnect(route, autocommit=True)
    with sql_connect.open():
        sql_connect.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")


def _drop_postgresql_database(route: Route) -> None:
    nodb_route = route.replace(database=None)
    nodb_sql_connect = SQLConnect(nodb_route, autocommit=True)
    try:
        nodb_sql_connect.execute(f"DROP DATABASE {route.database}")
    except psycopg2.errors.ObjectInUse:
        raise RuntimeError(f'Database "{route.database}" still has open connections') from None


def postgresql_via_docker(
    route: Route,
    postgresql_container: str = DEFAULT_POSTGRESQL_CONTAINER,
    postgresql_version: str = DEFAULT_POSTGRESQL_VERSION,
    postgresql_drop_db: bool = False,
    rm_container: bool = False,
) -> Iterator[Route]:
    # pylint: disable=unused-variable
    print("postgresql_via_docker")
    __tracebackhide__ = True
    docker_client = get_docker_client()
    container = get_docker_container(docker_client, postgresql_container)

    if container:
        print("existing container found. using it...")
        start_container = stop_container = container.status != "running"
        if container.ports and "5432/tcp" in container.ports:
            port_mapping = container.ports["5432/tcp"][0]
            host = port_mapping["HostIp"]
            if host == "0.0.0.0":
                host = "127.0.0.1"
            port = int(port_mapping["HostPort"])
            route = route.replace(host=host, port=port)
    else:
        print("no container found. downloading the image to start a new container")
        image_name = f"postgres:{postgresql_version}"
        container = docker_client.containers.run(
            image_name,
            name=postgresql_container,
            detach=True,
            ports={"5432/tcp": route.port},
            environment={
                "POSTGRES_PASSWORD": route.password,
            },
        )
        print("waiting till postgres is ready")
        SQLUtil.wait_postgresql_ready(route)
        _init_postgresql_container(route, container)
        start_container = False
        stop_container = True

    try:
        if start_container:
            container.start()
            SQLUtil.wait_postgresql_ready(route)

        _init_postgresql_database(route)
        yield route

    finally:
        if postgresql_drop_db:
            _drop_postgresql_database(route)

        if rm_container:
            container.remove(force=True)
        elif stop_container:
            container.stop()


@contextmanager
def postgresql_route() -> Iterator[Route]:
    if "PGHOST" in os.environ:
        # Use pre-provisioned PostgreSQL.
        yield from postgresql_via_env()

    else:
        # Use Docker container under our control.
        yield from postgresql_via_docker(DEFAULT_ROUTE)


def main():
    # TODO: now we need to use this route generator object in a new sql_connect object. Because route is a generator, the pg container will not run until we actually use route in a connection.
    # local_route = postgresql_via_docker(route=DEFAULT_ROUTE)
    # sql_connect = SQLConnect(local_route)
    # sql_connect.execute("")
    # sql_connect = SQLConnect(postgresql_route())

    with SQLConnect(postgresql_route()).open() as sql_connect:
        postgresql_version = sql_connect.select_value("SHOW server_version")
    assert postgresql_version.startswith("12.3 ")


if __name__ == "__main__":
    print("__main__")
    main()
