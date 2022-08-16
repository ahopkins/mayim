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
    """Interface for connecting to a Postgres database"""

    scheme = "postgres"

    def _setup_pool(self):
        if not POSTGRES_ENABLED:
            raise MayimError(
                "Postgres driver not found. Try reinstalling Mayim: "
                "pip install mayim[postgres]"
            )
        self._pool = AsyncConnectionPool(self.full_dsn)

    async def open(self):
        """Open connections to the pool"""
        await self._pool.open()

    async def close(self):
        """Close connections to the pool"""
        await self._pool.close()

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[AsyncConnection]:
        """Obtain a connection to the database

        Args:
            timeout (float, optional): Time before an error is raised on
                failure to connect. Defaults to `None`.

        Returns:
            AsyncIterator[Connection]: Iterator that will yield a connection

        Yields:
            Iterator[AsyncIterator[Connection]]: A database connection
        """
        existing = self._connection.get(None)
        if existing:
            yield existing
        else:
            async with self._pool.connection(timeout=timeout) as conn:
                yield conn
