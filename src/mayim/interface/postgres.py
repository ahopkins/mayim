from typing import AsyncContextManager, Optional

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from mayim.interface.base import BaseInterface


class PostgresPool(BaseInterface):
    scheme = "postgres"

    def _setup_pool(self):
        self._pool = AsyncConnectionPool(self.full_dsn)

    async def open(self):
        await self._pool.open()

    async def close(self):
        await self._pool.close()

    def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncContextManager[AsyncConnection]:
        return self._pool.connection(timeout=timeout)
