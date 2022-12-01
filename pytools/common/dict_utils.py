#!/usr/bin/env python

"""
Dictionary Tools
"""
import operator
import sys
from collections import defaultdict
from collections.abc import Collection
from functools import reduce
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

from pytools.common import list_utils
from pytools.common.logger import Logger
from pytools.common.string_utils import StringTools

logger = Logger(__name__)


#######################################
def get_nested_item(
    dict_obj: Mapping[str, Any], item_path: Iterable[str], raise_errors: bool = False
) -> Any:
    """
    Get nested `item_path` from `dict_obj`.

    Arguments:
        dict_obj -- Source dictionary.
        item_path -- Keys list.
        raise_errors -- Whether to raise `AttributeError` or `KeyError` if attribute/key not found

    Raises:
        AttributeError, KeyError -- If attribute/key not found.
    """
    try:
        return reduce(operator.getitem, item_path, dict_obj)
    except (AttributeError, KeyError) as e:
        if raise_errors:
            raise
        logger.exception(e, level=logger.DEBUG, show_traceback=False)
        return None


def recursive_merge_dict(base_dict: dict, new_dict: dict) -> dict:
    """
    Updates base dictionary including nested items, with values from new dictionary if present.
    Else adds the new key to base dictionary.

    Arguments:
        base_dict: Dictionary to used as base,
                   this dictionary will receive updates if key matches.
        new_dict: Dictionary that is used as new dictionary.
    Returns:
        A new dictionary as a merged dictionary, with updates on base from new.
    """

    result = {}
    result.update(base_dict)

    for key, value in new_dict.items():
        # Adding new key
        if key not in result:
            result[key] = value
            continue

        # Updating key with new value
        old_value = result[key]
        if not isinstance(value, dict):
            result[key] = value
            continue

        result[key] = recursive_merge_dict(old_value, value)
    return result


def dict_keys_snake_to_camel_case(dict_to_update: Dict[str, Any]) -> Dict[str, Any]:
    return {
        StringTools.convert_snake_case_to_camel_case(key): (
            dict_keys_snake_to_camel_case(value) if isinstance(value, dict) else value
        )
        for key, value in dict_to_update.items()
    }


def dict_keys_camel_to_snake_case(dict_to_update: Dict[str, Any]) -> Dict[str, Any]:
    return {
        StringTools.convert_camel_case_to_snake_case(key): (
            dict_keys_camel_to_snake_case(value) if isinstance(value, dict) else value
        )
        for key, value in dict_to_update.items()
    }


#######################################
def chunkify_keys(data: Mapping, size: int) -> Iterator[Dict[Any, Any]]:
    """
    Splits data to chunks of `size` length or less.

    ```python
    data = {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}
    for chunk in chunkify(data, size=2):
        print(chunk)

    # {'a': 1, 'b': 2}
    # {'c': 3, 'd': 4}
    # {'e': 5}
    ```

    Arguments:
        data -- Data to chunkify
        size -- Max chunk size.
    """
    for items in list_utils.chunkify(data=list(data.items()), size=size):
        yield dict(items)
