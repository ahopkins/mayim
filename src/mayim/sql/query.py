from __future__ import annotations

from enum import IntEnum, auto

from mayim.base.query import Query


class ParamType(IntEnum):
    NONE = auto()
    POSITIONAL = auto()
    KEYWORD = auto()


class SQLQuery(Query):
    __slots__ = ("name", "text", "param_type")
    text: str
    param_type: ParamType

    def __init__(self, name: str, text: str) -> None:
        self.name = name
        self.text = text

    def __str__(self) -> str:
        return (
            f"<{self.__class__.__name__} name={self.name} "
            f"text={self.text[:6]}... param_type={self.param_type.name}>"
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(name={self.name} "
            f"text={self.text[:6]}...)"
        )

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, SQLQuery)
            and self.text == other.text
            and self.param_type is other.param_type
        )

    def __add__(self, other: object) -> SQLQuery:
        if not isinstance(other, SQLQuery):
            raise ValueError(
                "SQLQuery can only be added with another SQLQuery"
            )
        if (
            self.param_type is not ParamType.NONE
            and other.param_type is not ParamType.NONE
            and self.param_type is not other.param_type
        ):
            raise ValueError(
                "Cannot combine queries with differing parameter types: "
                f"{self.param_type.name} and {other.param_type.name}"
            )
        text = self.text + other.text
        new_query = SQLQuery(name=self.name, text=text)
        new_query.param_type = max([self.param_type, other.param_type])
        return new_query
