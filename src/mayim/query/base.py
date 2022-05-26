from abc import ABC, abstractmethod
from enum import Enum, auto


class ParamType(Enum):
    POSITIONAL = auto()
    KEYWORD = auto()
    NONE = auto()


class Query(ABC):
    __slots__ = ()
    
    @abstractmethod
    async def covert_sql_params(self) -> str:
        ...