from typing import AsyncContextManager, Optional
from mayim.interface.base import BaseInterface
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool


class PostgresPool(BaseInterface):
    def __init__(self, dsn: str):
        self._pool = AsyncConnectionPool(dsn)

    async def open(self):
        await self._pool.open()

    async def close(self):
        await self._pool.close()

    def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncContextManager[AsyncConnection]:
        return self._pool.connection(timeout=timeout)
