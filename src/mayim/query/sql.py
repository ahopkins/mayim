from __future__ import annotations

from enum import Enum, auto

from .base import Query


class ParamType(Enum):
    POSITIONAL = auto()
    KEYWORD = auto()
    NONE = auto()


class SQLQuery(Query):
    __slots__ = ()
    text: str
    param_type: ParamType

    def __init__(self, text: str) -> None:
        ...

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} text={self.text[:6]}... "
            f"param_type={self.param_type}>"
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(text={self.text[:6]}...)"

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SQLQuery)
            and self.text == other.text
            and self.param_type is other.param_type
        )

    async def covert_sql_params(self) -> str:
        ...
