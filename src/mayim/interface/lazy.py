from typing import AsyncContextManager, Optional

from psycopg import AsyncConnection

from mayim.interface.base import BaseInterface


class LazyPool(BaseInterface):
    _singleton = None

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
        return cls._singleton

    async def open(self):
        ...

    async def close(self):
        ...

    def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncContextManager[AsyncConnection]:
        ...
