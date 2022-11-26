# Utility functions for writing DynamoDB-enabled tests in `tools` and other packages.
import os
from typing import Any, Iterator, NamedTuple
from unittest.mock import MagicMock, patch

from custody_py_tools.boto3_session_generator import Boto3SessionGenerator
from custody_py_tools.docker_utils import DockerContainer, get_docker_client, get_docker_container
from custody_py_tools.dynamo_connect import DynamoConnect

# Temporarily avoid `pytest` dependency to allow `pylint` to run without installing dev deps:
# import pytest


class Route(NamedTuple):
    host: str
    port: int

    @property
    def endpoint_url(self) -> str:
        return f"http://{self.host}:{self.port}"


def dynamodb_via_docker(
    container_name: str, port: int, version: str = "latest"
) -> Iterator[DockerContainer]:
    docker_client = get_docker_client()

    container: DockerContainer = get_docker_container(docker_client, container_name)
    if not container:
        image_name = f"amazon/dynamodb-local:{version}"
        new_container = docker_client.containers.run(
            image_name,
            name=container_name,
            detach=True,
            ports={"8000/tcp": port},
        )
        assert isinstance(new_container, DockerContainer)
        container = new_container

    initial_container_status = container.status
    if initial_container_status == "running":
        yield container
        return

    container.start()
    yield container
    container.stop()


# Temporarily avoid `pytest` dependency to allow `pylint` to run without installing dev deps:
# @pytest.fixture(scope="session")
# pylint: disable=unused-argument
def dynamodb_local(request: Any) -> Iterator[Route]:
    """
    Pytest-ready fixture for using local DynamoDB instance.

    Usage:

        ```python
        # conftest.py
        from tools.testing.dynamodb import dynamodb_local

        dynamodb_local = pytest.fixture(scope="session")(dynamodb_local)
        ```
    """
    # pylint: disable=unused-variable
    __tracebackhide__ = True

    container_name = "dynamodb-test"
    route = Route(host="localhost", port=28000)

    dc_autoscale_patch = patch.object(DynamoConnect, "autoscale_helper", MagicMock())
    dc_session_patch = patch.object(
        DynamoConnect, "boto3_session", Boto3SessionGenerator().generate_default_session()
    )
    dc_endpoint_patch = patch.object(DynamoConnect, "endpoint_url", route.endpoint_url)
    environ_patch = patch.object(
        os, "environ", {"AWS_ACCESS_KEY_ID": "none", "AWS_SECRET_ACCESS_KEY": "none", **os.environ}
    )
    patches = (dc_autoscale_patch, dc_endpoint_patch, dc_session_patch, environ_patch)
    for apply_patch in patches:
        apply_patch.start()  # type: ignore
    for _ in dynamodb_via_docker(container_name, route.port):
        yield route
    for apply_patch in patches:
        apply_patch.stop()  # type: ignore
