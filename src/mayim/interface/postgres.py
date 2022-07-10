from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

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

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[AsyncConnection]:
        existing = self._connection.get(None)
        if existing:
            yield existing
        else:
            async with self._pool.connection(timeout=timeout) as conn:
                yield conn
