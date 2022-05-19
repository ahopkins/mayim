from abc import ABC, abstractmethod
from typing import Optional


class BaseInterface(ABC):
    @abstractmethod
    async def open(self):
        ...

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    def connection(self, timeout: Optional[float] = None):
        ...
