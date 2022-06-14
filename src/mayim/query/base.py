from abc import ABC, abstractmethod


class Query(ABC):
    __slots__ = ()
    name: str

    @abstractmethod
    async def covert_sql_params(self) -> str:
        ...
