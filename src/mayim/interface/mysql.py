from typing import AsyncContextManager, Optional

import asyncmy

from mayim.interface.base import BaseInterface


class MysqlPool(BaseInterface):
    def __init__(self, dsn: str):
        self._pool = asyncmy.create_pool(dsn)

    async def open(self):
        self._pool = await self._pool

    async def close(self):
        self._pool.close()
        await self._pool.wait_closed()

    def connection(self, timeout: Optional[float] = None):
        # ) -> AsyncContextManager[AsyncConnection]:
        return self._pool.acquire()
