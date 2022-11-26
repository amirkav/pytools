import re
from dataclasses import dataclass
from inspect import cleandoc
from typing import Callable, Optional


def _str_plus_str(a: Optional[str], b: Optional[str], sep: str = "\n") -> Optional[str]:
    if a and b:
        return f"{a}{sep}{b}"
    return a or b


@dataclass
class DocString:
    description: Optional[str] = None
    arguments: Optional[str] = None
    yields: Optional[str] = None
    returns: Optional[str] = None

    def __post_init__(self) -> None:
        if self.description:
            self.description = self.description.strip("\n")
        if self.arguments:
            self.arguments = self.arguments.strip("\n")
        if self.yields:
            self.yields = self.yields.strip("\n")
        if self.returns:
            self.returns = self.returns.strip("\n")

    def __str__(self) -> str:
        doc = f"{self.description}\n" if self.description else ""
        if self.arguments:
            doc += f"\nArguments:\n{self.arguments}\n"
        if self.yields:
            doc += f"\nYields:\n{self.yields}\n"
        if self.returns:
            doc += f"\nReturns:\n{self.returns}\n"
        return doc.strip("\n")

    def __add__(self, other: "DocString") -> "DocString":
        if not isinstance(other, DocString):
            raise ValueError(f"Can add only DocString, got: {other!r}")
        return DocString(
            description=_str_plus_str(self.description, other.description, "\n\n"),
            arguments=_str_plus_str(self.arguments, other.arguments),
            yields=_str_plus_str(self.yields, other.yields),
            returns=_str_plus_str(self.returns, other.returns),
        )


_docstring_pattern = re.compile(
    r"""
    \A
    (?P<description> .*? )?
    \s*
    (?: Arguments:\n
        (?P<arguments> .*? )
    )?
    \s*
    (?: Yields:\n
        (?P<yields> .*? )
    )?
    \s*
    (?: Returns:\n
        (?P<returns> .*? )
    )?
    \Z
    """,
    re.X | re.S,
)


def parse_docstring(doc: str) -> DocString:
    doc = cleandoc(doc)
    match = _docstring_pattern.match(doc)
    if match:
        return DocString(
            description=match["description"],
            arguments=match["arguments"],
            yields=match["yields"],
            returns=match["returns"],
        )
    raise ValueError(f"Unable to parse docstring: {doc!r}")


def documented_by(original: Callable) -> Callable:
    def decorator(target: Callable) -> Callable:
        if original.__doc__ and target.__doc__:
            target.__doc__ = str(
                parse_docstring(original.__doc__) + parse_docstring(target.__doc__)
            )
        elif original.__doc__:
            target.__doc__ = original.__doc__
        return target

    return decorator
