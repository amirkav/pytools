import functools
import inspect
import warnings
from typing import Any, Callable, Optional, TypeVar, Union, cast, overload

__all__ = ["deprecated"]


FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


class deprecated:
    """
    Mark class or function as deprecated.

    Arguments:
        argument -- Deprecated argument name.
        reason -- Extra message to help resolving the issue.

    Examples:

        ```python
        @deprecated()
        def deprecated_func(name: str) -> str:
            return f"Hello, {name}"

        @deprecated(reason="use func3 instead")
        def deprecated_func2(name: str) -> str:
            return f"Hello, {name}"

        @deprecated(argument="last_name", reason="Use name instead")
        @deprecated(argument="verbose")
        def deprecated_func3(name: str, last_name: str = "", verbose: bool = False) -> str:
            return f"Hello, {name}"

        @deprecated()
        class MyDeprecatedClass: pass
        ```
    """

    NONE: Callable[..., Any] = lambda x: x

    def __init__(self, argument: Union[str, FunctionType] = "", reason: str = ""):
        self._reported = False
        self._reason = reason
        self._argument = ""
        self._obj: Optional[FunctionType] = None

        if isinstance(argument, str):
            self._argument = argument
            return

        self._obj = argument

    @property
    def _stacklevel(self) -> int:
        stacklevel = 0
        frame = inspect.currentframe()
        deprecated_wrapped_found = False
        while frame:
            frame_info = inspect.getframeinfo(frame)
            if frame_info.function == "deprecated_wrapper":
                deprecated_wrapped_found = True
            if deprecated_wrapped_found and frame_info.function != "deprecated_wrapper":
                break
            frame = frame.f_back
            stacklevel += 1
        return stacklevel

    @property
    def _reason_postfix(self) -> str:
        if self._reason:
            return f": {self._reason}"

        return ""

    def _warn(self, msg: str) -> None:
        warnings.warn(msg, category=DeprecationWarning, stacklevel=self._stacklevel)

    def _call(self, obj: FunctionType) -> FunctionType:
        @functools.wraps(obj)
        def deprecated_wrapper(*args: Any, **kwargs: Any) -> Any:
            if self._argument:
                if self._argument in kwargs:
                    if not self._reported:
                        obj_name = obj.__name__ if obj else "<unknown>"
                        obj_type = "class" if inspect.isclass(obj) else "function"
                        self._warn(
                            f"Usage of deprecated argument '{self._argument}' in {obj_type}"
                            f" {obj_name}{self._reason_postfix}"
                        )
                        self._reported = True
                return obj(*args, **kwargs)

            if not self._reported:
                obj_name = obj.__name__ if obj else "<unknown>"
                obj_type = "class" if inspect.isclass(obj) else "function"
                self._warn(f"Call to deprecated {obj_type} {obj_name}{self._reason_postfix}")
                self._reported = True

            return obj(*args, **kwargs)

        return cast(FunctionType, deprecated_wrapper)

    # pylint: disable=keyword-arg-before-vararg
    @overload
    def __call__(self, _call_obj: FunctionType = ..., *args: Any, **kwargs: Any) -> FunctionType:
        ...

    # pylint: disable=keyword-arg-before-vararg
    @overload
    def __call__(self, _call_obj: Any = ..., *args: Any, **kwargs: Any) -> Any:
        ...

    # pylint: disable=keyword-arg-before-vararg
    def __call__(
        self, _call_obj: Union[FunctionType, Any] = NONE, *args: Any, **kwargs: Any
    ) -> FunctionType:
        if self._obj:
            call_args = list(args)
            if _call_obj != self.NONE:
                call_args.insert(0, _call_obj)
            return self._call(self._obj)(*call_args, **kwargs)

        if not _call_obj:
            raise ValueError("Deprecated callable was not passed")

        call_obj = cast(FunctionType, _call_obj)
        return self._call(call_obj)


def main() -> None:
    @deprecated
    def deprecated_func(name: str) -> str:
        return f"Hello, {name}"

    @deprecated(reason="Use any other function")
    def deprecated_func2(name: str) -> str:
        return f"Hello, {name}"

    @deprecated(reason="Deprecated class")
    class MyDeprecatedClass:
        pass

    @deprecated(argument="name")
    @deprecated(argument="last_name", reason="Use name instead")
    @deprecated(argument="verbose")
    def deprecated_func3(name: str, last_name: str = "", verbose: bool = False) -> str:
        return f"Hello, {name} {last_name} {verbose}"

    for _i in range(3):
        deprecated_func("John")
    deprecated_func("John")
    deprecated_func2("Steve")
    deprecated_func3(name="Steve", last_name="John", verbose=True)

    MyDeprecatedClass()


if __name__ == "__main__":
    main()
