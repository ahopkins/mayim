from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

try:
    from psycopg import AsyncConnection
    from psycopg_pool import AsyncConnectionPool

    POSTGRES_ENABLED = True
except ModuleNotFoundError:
    POSTGRES_ENABLED = False
    AsyncConnection = type("Connection", (), {})  # type: ignore
    AsyncConnectionPool = type("Connection", (), {})  # type: ignore


class PostgresPool(BaseInterface):
    scheme = "postgres"

    def _setup_pool(self):
        if not POSTGRES_ENABLED:
            raise MayimError(
                "Postgres driver not found. Try reinstalling Mayim: "
                "pip install mayim[postgres]"
            )
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
