import re

from mayim.exception import MayimError
from mayim.sql.query import ParamType, SQLQuery


class MysqlQuery(SQLQuery):
    __slots__ = ("name", "text", "param_type")
    PATTERN_POSITIONAL_PARAMETER = re.compile(r"%s")
    PATTERN_KEYWORD_PARAMETER = re.compile(r"%\([a-z_][a-z0-9_]*\)")

    def __init__(self, name: str, text: str) -> None:
        super().__init__(name, text)
        positional_argument_exists = bool(
            self.PATTERN_POSITIONAL_PARAMETER.search(self.text)
        )
        keyword_argument_exists = bool(
            self.PATTERN_KEYWORD_PARAMETER.search(self.text)
        )

        if positional_argument_exists and keyword_argument_exists:
            raise MayimError("Positional and keyword arguments provided")
        if positional_argument_exists:
            self.param_type = ParamType.POSITIONAL
        elif keyword_argument_exists:
            self.param_type = ParamType.KEYWORD
        else:
            self.param_type = ParamType.NONE
