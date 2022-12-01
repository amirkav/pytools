#!/usr/bin/env python
from itertools import islice
from typing import Any, Iterable, Iterator, List


def lists_overlap(a: Iterable, b: Iterable) -> bool:
    """
    Check if there are common items between the two lists.

    Arguments:
        a -- First list of values.
        b -- Second list of values.

    Returns:
        A bool flag that is set to True if the two lists overlap.
    """
    return bool(set(a) & set(b))


def list_items_equal(a: Iterable) -> bool:
    """
    Checks whether all items in the list are equal
    For more implementation and potential optimizations, see:
    https://stackoverflow.com/questions/3844801/check-if-all-elements-in-a-list-are-identical

    Arguments:
        a -- List to check.

    Returns:
        True if all items in the list are equal
    """
    return len(set(a)) == 1


def chunkify(data: Iterable, size: int) -> Iterator[List[Any]]:
    """
    Splits data to chunks of `size` length or less.

    ```python
    data = [1, 2, 3, 4, 5]
    for chunk in chunkify(data, size=2):
        print(chunk)

    # [1, 2]
    # [3, 4]
    # [5]
    ```

    Arguments:
        data -- Data to chunkify
        size -- Max chunk size.

    Returns:
        A generator of chunks.
    """
    iterator = iter(data)
    return iter(lambda: list(islice(iterator, size)), [])


def isinstance_list_like(value: Any) -> bool:
    """
    Check if `value` is a `list`, `tuple` or `set`.

    ```python
    list_utils.isinstance_list_like([1, 2]) # True
    list_utils.isinstance_list_like({1, 2}) # True
    list_utils.isinstance_list_like((1, 2)) # True
    list_utils.isinstance_list_like('asd') # False
    list_utils.isinstance_list_like(123) # False
    ```

    Arguments:
        value -- Item to check.

    Returns:
        True if `value` is list-like.
    """
    return isinstance(value, (list, set, tuple))
