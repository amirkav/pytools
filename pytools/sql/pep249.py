from typing import AnyStr, Iterator, Mapping, Optional, Sequence, Tuple, TypeVar, Union

from pytools.type_defs import Protocol, runtime_checkable

Params = Union[Sequence, Mapping]
SeqRow = Sequence
MapRow = Mapping

RowT = TypeVar("RowT", SeqRow, MapRow, covariant=True)


class Cursor(Protocol[RowT]):
    description: Tuple[str, type, int, int, int, int, bool]
    rowcount: int
    arraysize: int

    # These `pylint` controls are needed due to <https://github.com/PyCQA/pylint/issues/3885>.
    # pylint: disable=no-self-use
    def callproc(self, __procname: str, __parameters: Optional[Params] = ...) -> None:
        ...

    # pylint: disable=no-self-use
    def close(self) -> None:
        ...

    # pylint: disable=no-self-use
    def execute(self, __operation: str, __parameters: Optional[Params] = ...) -> None:
        ...

    # pylint: disable=no-self-use
    def executemany(self, __operation: str, __seq_of_parameters: Sequence[Params] = ...) -> None:
        ...

    # pylint: disable=no-self-use
    def fetchone(self) -> Optional[RowT]:
        ...

    # pylint: disable=no-self-use
    def fetchmany(self, __size: int = ...) -> Sequence[RowT]:
        ...

    # pylint: disable=no-self-use
    def fetchall(self) -> Sequence[RowT]:
        ...

    # pylint: disable=no-self-use
    def mogrify(self, __operation: str, __parameters: Optional[Params] = ...) -> AnyStr:
        ...

    def __iter__(self) -> Iterator[RowT]:  # pylint: disable=non-iterator-returned
        ...


class TupleCursor(Cursor[SeqRow]):
    ...


class DictCursor(Cursor[MapRow]):
    ...


@runtime_checkable
class Connection(Protocol):
    def close(self) -> None:
        ...

    def commit(self) -> None:
        ...

    def rollback(self) -> None:
        ...

    # PEP 249 doesn't specify the arguments for this method, and `mypy` currently offers no way to
    # type "arbitrary arguments". One might think the following matches any method named `cursor`,
    # no matter its arguments, but it does not. <https://github.com/python/mypy/issues/5876>
    # So we omit this method from the protocol.
    # def cursor(self, *__args: Any, **__kwargs: Any) -> Cursor:
    #     ...
