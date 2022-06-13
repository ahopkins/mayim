from abc import ABC, abstractmethod


class Query(ABC):
    __slots__ = ()

    @abstractmethod
    async def covert_sql_params(self) -> str:
        ...
