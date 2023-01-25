import time
from contextlib import contextmanager
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, create_autospec, patch

import moto
import pytest

import pytools.testing
from pytools.dynamodb_fixtures import dynamodb_local
from pytools.sql_fixtures import (
    mock_ro_sql_route,
    mock_rw_sql_route,
    mysql_route,
    postgresql_route,
    sql_connect,
    sql_route,
)
from pytools.catalogs import Platform
from pytools.config import Config
from pytools.dynamo.config_manager import ConfigsInterface
from pytools.param_store import ParamStore
from pytools.retry_backoff_class import RetryAndCatch
from pytools.s3_connect import S3Connect

__all__ = (
    "mysql_route",
    "postgresql_route",
    "dynamodb_local",
    "mock_config",
    "mock_param_store",
    "mock_ro_sql_route",
    "mock_rw_sql_route",
)


def pytest_addoption(parser):
    pytools.testing.register_cli_options_rm_containers(parser)
    pytools.testing.register_cli_options_mysql(parser)
    pytools.testing.register_cli_options_postgresql(parser)


# Universal test resources
###################################################################################################


def assert_dict_contains(d: dict, expected: Optional[dict] = None) -> None:
    __tracebackhide__ = True
    missing = object()
    if not isinstance(expected, dict):
        pytest.fail(f"Not a dict: {d}")
    diff = {}
    for key in expected.keys():
        if not key in d:
            diff[key] = (missing, expected[key])
        elif d[key] != expected[key]:
            diff[key] = (d[key], expected[key])
    if diff:
        key_length = max(len(key) for key in diff.keys())
        lines = ["dict keys mismatch:", "{"]
        for key, (value, expected) in diff.items():
            lines.append(
                f"  {repr(key) + ':':<{key_length + 3}} "
                f"{'missing' if value is missing else repr(value)} != {expected!r}"
            )
        lines.append("}")
        pytest.fail("\n".join(lines))


# Per <https://github.com/pytest-dev/pytest/issues/363#issuecomment-406536200>:
@pytest.fixture(scope="session")
def session_monkeypatch(request):
    from _pytest.monkeypatch import MonkeyPatch

    session_monkeypatch = MonkeyPatch()
    yield session_monkeypatch
    session_monkeypatch.undo()


@pytest.fixture(scope="class")
def class_monkeypatch(request):
    from _pytest.monkeypatch import MonkeyPatch

    class_monkeypatch = MonkeyPatch()
    yield class_monkeypatch
    class_monkeypatch.undo()


# Unit test resources
###################################################################################################


@pytest.fixture(autouse=True, scope="session")
def disable_aws(request, session_monkeypatch):
    if request.node.get_closest_marker("integration"):
        # Do not disable AWS for integration tests.
        pass
    else:
        session_monkeypatch.delenv("AWS_PROFILE", raising=False)
        session_monkeypatch.setenv("AWS_ACCESS_KEY_ID", "none")
        session_monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "none")
        session_monkeypatch.setenv("AWS_SESSION_TOKEN", "none")


@pytest.fixture
def no_retry():
    with RetryAndCatch.no_retry():
        yield


@pytest.fixture
def no_backoff():
    with RetryAndCatch.no_backoff():
        yield


@pytest.fixture
def mock_config():
    config = MagicMock(spec=Config)

    config.project_name = "test"
    config.env = "test"
    config.aws_account_id = "123456789"
    config.aws_region = "aws_region"
    config.s3_bucket = f"test-{config.project_name}-{config.env}"
    config.sentry_dsn = "https://sentry_dsn"
    config.suffix = "suffix"
    config.platforms_as_enum = {Platform.GSUITE}

    # Read-only DB access:
    config.db_ro_endpoint = "test-mysql-ro.us-west-2.rds.amazonaws.com"
    config.pg_db_ro_endpoint = "test-pg-ro.us-west-2.rds.amazonaws.com"
    config.readonly_user = "readonly_user"

    # Read-write DB access:
    config.endpoint = "test-mysql.us-west-2.rds.amazonaws.com"
    config.pg_db_endpoint = "test-pg.us-west-2.rds.amazonaws.com"
    config.readwrite_user = "readwrite_user"

    # Master-user DB access:
    config.master_user = "master_user"
    config.master_pass_parameter = "/…/db_master_pass"
    config.db_password = "master_password"

    config.database_type = "MYSQL"
    config.db_name = "db_name"
    config.db_port = None
    config.postgresql_db_port = None
    config.db_iam_auth = True

    config.rds_cert_parameter = "/…/rds_cert"
    config.cors_domains = set("*")

    # GSuite authentication json file
    config.gs_sa_json_filename = "thoughtlabs-737b20aa54f9.json"
    # O365 authentication json file
    config.ms_sa_json_filename = "thoughtlabs-737b20aa54f9.json"

    return config


@pytest.fixture
def mock_boto3_session():
    return MagicMock()


class MockParamStore(ParamStore):
    def get_param(self, param_name, encrypted):
        if param_name.endswith("/db_master_pass"):
            return "master_password"

        if param_name.endswith("/rds_cert"):
            return "rds_cert"

        return True


@pytest.fixture
def mock_param_store():
    boto3_session_mock = MagicMock()
    return MockParamStore(aws_region="us-west-2", boto3_session=boto3_session_mock)


@pytest.fixture
def mock_warnings():
    with patch("tools.deprecated.warnings") as mock:
        yield mock


@pytest.fixture(scope="session")
def mock_site_verification_resource() -> Any:
    def mocked_func(**_kwargs: Any) -> Dict:
        return {"success": True}

    services = {
        "getToken": type("getToken", (), {"execute": mocked_func}),
        "insert": type("insert", (), {"execute": mocked_func}),
        "get": type("get", (), {"execute": mocked_func}),
        "delete": type("delete", (), {"execute": mocked_func}),
    }

    Service = type("Service", (), services)
    return create_autospec(Service)


@pytest.fixture
def local_s3_bucket():
    return "test-altitude-local-01"


@pytest.fixture
def mock_s3_connect(local_s3_bucket):
    with moto.mock_s3():
        s3_connect = S3Connect(aws_region="us-west-2")
        s3_connect.create_bucket(bucket=local_s3_bucket)
        yield s3_connect
        s3_connect.delete_all_objects(local_s3_bucket)
        s3_connect.delete_bucket(local_s3_bucket)


@pytest.fixture
def local_config(dynamodb_local, mock_config, mock_s3_connect):
    project_name = mock_config.project_name
    env = "local"
    aws_region = "us-west-2"

    config_record_template = {
        "project_name": project_name,
        "status": "active",
    }

    config_manager = ConfigsInterface(
        project_id=project_name,
        aws_region=aws_region,
        env=env,
        template=config_record_template,
        s3_connect=mock_s3_connect,
    )
    config_manager.create_table()
    config_manager.create()

    yield Config(project_name=project_name, env=env, aws_region=aws_region)

    config_manager.delete_table()
