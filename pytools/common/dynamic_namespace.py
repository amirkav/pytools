import re
from typing import Any, Callable, List, Optional, Sequence, Set, Tuple


class DynamicNamespace:
    def __init__(
        self,
        _get: Callable,
        _set: Optional[Callable] = None,
        _del: Optional[Callable] = None,
        _dir: Optional[Callable] = None,
        path: List[str] = None,
    ) -> None:
        self._get = _get
        self._set = _set
        self._del = _del
        self._dir = _dir
        self._path = path or []

    @property
    def _value(self) -> Any:
        try:
            return self._get(self._path)
        except AttributeError as e:
            raise AttributeError(f"Namespace has no path '{'.'.join(self._path)}': {e}") from e

    def _ensure_path(self, *path: str) -> None:
        node = self
        for segment in path:
            try:
                node = node[segment]
            except AttributeError:
                node[segment] = {}
                node = node[segment]

    def _items(self) -> Set[Tuple[str, Any]]:
        return {(name, self[name]) for name in self._value}

    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)

    def __setitem__(self, name: str, value: Any) -> None:
        self.__setattr__(name, value)

    def __getattr__(self, name: str) -> Any:
        path = self._path + [name]
        try:
            value = self._get(path)
        except AttributeError as e:
            raise AttributeError(f"Namespace has no path '{'.'.join(path)}': {e}") from e
        if isinstance(value, dict):
            return self.__class__(
                _get=self._get, _set=self._set, _del=self._del, _dir=self._dir, path=path
            )
        return value

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            super().__setattr__(name, value)
        else:
            if self._set:
                path = self._path + [name]
                try:
                    self._set(path, value)
                except AttributeError as e:
                    # This should not occur, as non-existent (nx) nodes in an attribute path like
                    # `a.b.nx.c = 42` will raise in the getter before we even get to the setter.
                    # We keep this exception handler for symmetry and as an opportune place to
                    # explain these mechanics.
                    raise AttributeError(f"Namespace has no path '{'.'.join(path)}': {e}") from e
            else:
                raise AttributeError("Namespace is read-only")

    def __delitem__(self, name: str) -> None:
        if self._del:
            path = self._path + [name]
            try:
                self._del(path)
            except AttributeError as e:
                raise AttributeError(f"Namespace has no path '{'.'.join(path)}': {e}") from e
        else:
            raise AttributeError("Namespace is read-only")

    def __dir__(self) -> Sequence[str]:
        _base_dir = [a for a in super().__dir__() if not re.match(r"^_[^_]", a)]
        _dir = self._dir(self._path) if self._dir else []
        return [*_base_dir, *_dir]
