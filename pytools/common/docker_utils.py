import docker
import requests
from docker.client import DockerClient
from docker.models.containers import Container as DockerContainer


def get_docker_container(
    docker_client: docker.DockerClient, container_name: str
) -> DockerContainer:
    """
    Get container by `container_name`.

    Returns:
        A container object or None.
    """
    try:
        return docker_client.containers.get(container_name)

    except docker.errors.NotFound:
        return None

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise

    except requests.exceptions.ChunkedEncodingError:
        # Bug in Docker Desktop 2.5.0 or in `docker-py`:
        # <https://github.com/docker/docker-py/issues/2696>
        return None


def get_docker_client() -> DockerClient:
    """
    Get Docker client from environment.

    Returns:
        DockerClient object.

    Raises:
        `RuntimeError` if docker is not available.
    """
    docker_client = docker.from_env()
    try:
        docker_client.ping()
    except (docker.errors.APIError, requests.exceptions.ConnectionError) as e:
        raise RuntimeError(f"Docker not available: {e}") from None

    return docker_client
