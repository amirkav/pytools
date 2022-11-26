import pytest

from custody_py_tools.postgresql_local import postgresql_route
from custody_py_tools.sql import Route, SQLConnect


class TestPostgresqlRouteCtxMgr:
    @pytest.fixture(scope="class")
    def route(self):
        with postgresql_route() as route:
            yield route

    def test_postgresql_route(self, route) -> None:
        assert isinstance(route, Route)
        with SQLConnect(route).open() as sql_connect:
            postgresql_version = sql_connect.select_value("SHOW server_version")
        assert postgresql_version.startswith("12.3 ")

    def test_masteruser_has_create_user_privilege(self, route) -> None:
        with SQLConnect(route).open() as sql_connect:
            sql_connect.execute("CREATE USER test_dummyuser PASSWORD 'nopasswd'")
            sql_connect.execute("DROP USER test_dummyuser")
