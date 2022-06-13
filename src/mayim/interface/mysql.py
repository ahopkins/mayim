from typing import AsyncContextManager, Optional

import asyncmy

from mayim.interface.base import BaseInterface


class MysqlPool(BaseInterface):
    def _setup_pool(self):
        self._pool = asyncmy.create_pool(
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            db=self.db,
        )

    async def open(self):
        self._pool = await self._pool

    async def close(self):
        self._pool.close()
        await self._pool.wait_closed()

    def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncContextManager[asyncmy.contexts._PoolAcquireContextManager]:
        return self._pool.acquire()
