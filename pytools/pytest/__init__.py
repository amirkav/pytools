from typing import Any

from .dynamodb import dynamodb_local
from .postgresql import postgresql_route, register_cli_options_postgresql

__all__ = (
    "register_cli_options_rm_containers",
    "register_cli_options_postgresql",
    "dynamodb_local",
    "postgresql_route",
)


pytest_ArgParser = Any


def register_cli_options_rm_containers(parser: pytest_ArgParser) -> None:
    """
    Add pytest `--rm-containers` command-line option for requesting tear-down of all containers
    after test run.
    """
    parser.addoption(
        "--rm-containers",
        action="store_true",
        help="Tear down containers after completion.",
    )
