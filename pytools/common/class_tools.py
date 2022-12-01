import time
from typing import Any, Callable, Generic, Type, TypeVar, Union, overload

from pytools.common.logger import Logger

__all__ = ("cached_property",)

_S = TypeVar("_S")
_T = TypeVar("_T")


# <https://stackoverflow.com/a/13624858/454126>
class class_property:
    def __init__(self, fget: Callable) -> None:
        self.fget = fget

    def __get__(self, owner_self: Any, owner_cls: Type) -> Any:
        return self.fget(owner_cls)


class cached_property(Generic[_S, _T]):
    """
    Compute the property once and cache it (Thread-unsafe)

    Arguments:
        ttl (int) -- Invalidate the cache after ttl seconds and recompute the property

    Example:

        ```python

        class MyClass:
            @cached_property
            def slow_prop(self) -> str:
                return slow_api.get()

            @cached_property(ttl=5)
            def slow_prop2(self) -> str:
                return slow_api.get()

        a = MyClass()
        logger.debug(a.slow_prop) # returns a regular result
        logger.debug(a.slow_prop) # returns cached result
        logger.debug(a.slow_prop2) # returns a regular result
        logger.debug(a.slow_prop2) # returns cached result
        time.sleep(10)
        logger.debug(a.slow_prop2) # returns a regular result
        ```
    """

    attr_template = "__cached_%s"

    def __init__(self, ttl: Union[Callable[..., _T], int]) -> None:
        _func, _ttl = (ttl, None) if callable(ttl) else (None, ttl)
        self.ttl = _ttl
        self._prepare_func(func=_func)

    def _prepare_func(self, func: Any) -> None:
        self.func = func
        if func:
            self.func_name = func.__name__
            self.attrname: str = self.attr_template % self.func_name
            self.__doc__ = func.__doc__

    def __call__(self, func: Any) -> "cached_property":
        self._prepare_func(func)
        return self

    @overload
    def __get__(self, instance: None, cls: Any) -> "cached_property":
        ...

    @overload
    def __get__(self, instance: object, cls: Any) -> _T:
        ...

    def __get__(self, instance: Any, cls: Any) -> Union["cached_property", _T]:
        if instance is None:
            return self

        cache = self.__dict__
        if instance is not None:
            cache = instance.__dict__

        now = time.monotonic()
        try:
            value, last_updated = cache[self.attrname]
        except KeyError:
            pass
        else:
            ttl_expired = self.ttl and self.ttl < now - last_updated
            if not ttl_expired:
                return value

        value = self.func(instance)
        cache[self.attrname] = (value, now)
        return value


def main() -> None:
    logger = Logger.main(level=Logger.DEBUG)

    class MyClass:
        name = "MyClass"

        @cached_property
        def slow_prop(self) -> str:
            logger.debug("slow_prop: no cache")
            return self.name

        @cached_property
        def slow_prop2(self) -> str:
            logger.debug("slow_prop2: no cache")
            return f"{self.name}!"

        @cached_property(ttl=1)
        def slow_prop3(self) -> str:
            logger.debug("slow_prop3: no cache")
            return f"{self.name}!!"

    a = MyClass()
    logger.debug(a.slow_prop)
    logger.debug(a.slow_prop2)
    logger.debug(a.slow_prop)
    logger.debug(a.slow_prop2)
    logger.debug(a.slow_prop3)
    logger.debug(a.slow_prop3)
    time.sleep(2)
    logger.debug(a.slow_prop3)


if __name__ == "__main__":
    main()
