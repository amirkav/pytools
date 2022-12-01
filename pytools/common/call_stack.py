import inspect
from types import ModuleType
from typing import List, Optional


def getcaller(
    context: int = 1,
    ignore_filenames: Optional[List[str]] = None,
    ignore_modules: Optional[List[ModuleType]] = None,
) -> inspect.FrameInfo:
    """
    Inspects the current call stack and returns the first frame, the immediate caller.

    If `ignore_filenames` and/or `ignore_modules` are given, skips any frames whose
    filename matches any of the filenames given or the filename of any of the modules
    given; this is useful for ignoring callers internal to a library.

    Arguments:
        context: int -- How many lines of context to include in each frame's
            `code_context` attribute (default: 1).
        ignore_filenames: List[str] -- (optional) Ignore frames from these files.
        ignore_modules: List[ModuleType] -- (optional) Ignore frames from these
            modules.

    Returns:
        `inspect.FrameInfo` object describing the desired caller.
    """
    ignore_filenames = ignore_filenames or []
    ignore_modules = ignore_modules or []
    ignore_filenames = ignore_filenames + [__file__] + [m.__file__ for m in ignore_modules]
    # Ensure that an "ignore" filename of `bar.py` won't unexpectedly match, say, `/lib/foobar.py`:
    ignore_filenames = [ifn if ifn.startswith("/") else f"/{ifn}" for ifn in ignore_filenames]
    call_stack = [
        frameinfo
        for frameinfo in inspect.stack(context=context)
        if not any(frameinfo.filename.endswith(ifn) for ifn in ignore_filenames)
    ]
    return call_stack[0]
