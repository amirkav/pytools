import datetime
import decimal
from pathlib import Path
from typing import Any, Set, TextIO, Union, Iterable

import yaml
from yaml.dumper import SafeDumper
from yaml.nodes import Node
from yaml.representer import SafeRepresenter


class CustomSafeRepresenter(SafeRepresenter):
    """
    Representer that keeps data consistent with `json_tools.SafeJSONEncoder`.

    Defferences from `yaml.SafeRepresenter`:

    - set is serialized to a sorted list
    - date is serialized to a string in "%Y-%m-%d" format
    - datetime is serialized to a string in "%Y-%m-%dT%H:%M:%SZ" format
    - integral Decimal is serialized to int
    - non-integral Decimal is serialized to float
    - Exception is serialized to string
    - Unknown type is serialized to string as a repr
    """

    iso_format = r"%Y-%m-%dT%H:%M:%SZ"
    simple_date_format = r"%Y-%m-%d"

    def represent_set(self, data: Set[Any]) -> Node:
        """
        Serialize set to list Node.
        """
        return self.represent_list(sorted(data))

    def represent_date(self, data: datetime.date) -> Node:
        """
        Serialize date to string Node.
        """
        return self.represent_str(data.strftime(self.simple_date_format))

    def represent_datetime(self, data: datetime.datetime) -> Node:
        """
        Serialize datetime to string Node.
        """
        return self.represent_str(data.strftime(self.iso_format))

    def represent_decimal(self, data: decimal.Decimal) -> Node:
        """
        Serialize Decimal to int or float Node.
        """
        if data == data.to_integral_value():
            return self.represent_int(int(data))

        return self.represent_float(float(data))

    def represent_exception(self, data: BaseException) -> Node:
        """
        Serialize Exception to string Node.
        """
        return self.represent_str(f"{data.__class__.__name__}('{data}')")

    def represent_undefined(self, data: Any) -> Node:
        """
        Serialize unknown type to string Node.
        """
        # FIXME: py36 support
        if isinstance(data, BaseException):
            return self.represent_exception(data)

        return self.represent_str(repr(data))


# pylint: disable=too-many-ancestors
class CustomSafeDumper(SafeDumper, CustomSafeRepresenter):
    """
    Dumper that keeps data consistent with `json_tools.SafeJSONEncoder`.

    Uses `CustomSafeRepresenter` overrides.
    """

    # add custom representers to this dict
    yaml_representers = {
        **SafeDumper.yaml_representers,
        None: CustomSafeRepresenter.represent_undefined,
        set: CustomSafeRepresenter.represent_set,
        datetime.date: CustomSafeRepresenter.represent_date,
        datetime.datetime: CustomSafeRepresenter.represent_datetime,
        BaseException: CustomSafeRepresenter.represent_exception,
        decimal.Decimal: CustomSafeRepresenter.represent_decimal,
    }


def dump(data: Any, force_block_style: bool = True) -> str:
    """
    Alias for `yaml.dump`.

    Arguments:
        data -- JSON-serializable object.
        force_block_style -- Always serialize collection in the block style.

    Returns:
        A string with serialized YAML.
    """
    return yaml.dump(data, Dumper=CustomSafeDumper, default_flow_style=not force_block_style)


def load(data: Union[str, TextIO]) -> Any:
    """
    Alias for `yaml.load`.

    Arguments:
        data -- A string on readable IO with valid YAML.

    Returns:
        An object created from YAML data.
    """
    return yaml.safe_load(data)


def load_from_file(file_path: Path) -> Any:
    """Loads yaml from a given file path

    Arguments:
        file_path -- Path object of existing `.yml` file.

    Returns:
        An object created from YAML data.
    """
    return load(file_path.read_text())


def load_all(data: Union[str, TextIO]) -> Iterable[dict]:
    """
    Alias for `yaml.safe_load_all`.

    Arguments:
        data -- A string on readable IO with valid YAML. Can process multiple YAML documents in one file.

    Returns:
        An object created from YAML data.
    """
    gen = yaml.safe_load_all(data)
    results = []
    for d in gen:
        results.append(d)
    return results


def load_all_from_file(file_path: Path) -> Iterable[dict]:
    """Loads yaml from a given file path
    Can handle YAML files containing multiple documents.

    Arguments:
        file_path -- Path object of existing `.yml` file.

    Returns:
        An object created from YAML data.
    """
    return load_all(file_path.read_text())


def dump_to_file(data: Any, file_path: Path, force_block_style: bool = True) -> None:
    """Dumps collection contents to a given yml file path

    Arguments:
        data: JSON-serializable object.
        file_path: Path object used for yaml dump (e.g. `dump.yml`).
        force_block_style: Always serialize collection in the block style.
        For example:
        ```python
            my_dict = {
                "config": {
                    "a": "a",
                    "b": "b"
                }
            }
            yaml_tools.dump_to_file(my_dict, "some/path/my_dict.yml")
        ```
        This results in `my_dict.yml` with the following contents in block style.
        ```yml
        config:
            a: a
            b: b
        ```
    """
    with file_path.open("w+") as output:
        yaml.dump(
            data,
            stream=output,
            default_flow_style=not force_block_style,
            Dumper=CustomSafeDumper,
            width=float("inf"),
        )


if __name__ == "__main__":
    my_dict = {"config": {"a": "a", "b": "b"}}
    print(dump(my_dict))
