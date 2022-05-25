import re
from enum import Enum, auto

from mayim.exception import MayimError

PATTERN_POSITIONAL_PARAMETER = re.compile(r"\$\d+(?![a-z0-9_])")
PATTERN_KEYWORD_PARAMETER = re.compile(r"\$\d*[a-z_][a-z0-9_]*")


class ParamType(Enum):
    POSITIONAL = auto()
    KEYWORD = auto()
    NONE = auto()


class Query:
    def __init__(self, text: str) -> None:
        self.text = text
        positional_argument_exists = bool(
            PATTERN_POSITIONAL_PARAMETER.search(self.text)
        )
        keyword_argument_exists = bool(
            PATTERN_KEYWORD_PARAMETER.search(self.text)
        )

        if positional_argument_exists and keyword_argument_exists:
            raise MayimError("Positional and keyword arguments provided")
        if positional_argument_exists:
            self.param_type = ParamType.POSITIONAL
        elif keyword_argument_exists:
            self.param_type = ParamType.KEYWORD
        else:
            self.param_type = ParamType.NONE

    def convert_sql_params(self) -> str:
        converted_text = self.text.replace("%", "%%")
        if self.param_type == ParamType.POSITIONAL:
            return PATTERN_POSITIONAL_PARAMETER.sub(r"%s", converted_text, 0)
        if self.param_type == ParamType.KEYWORD:
            return PATTERN_KEYWORD_PARAMETER.sub(r"%(\2)s", converted_text, 0)
        return converted_text
