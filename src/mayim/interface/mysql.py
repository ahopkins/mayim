from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncContextManager, Optional

import asyncmy

from mayim.interface.base import BaseInterface


class MysqlPool(BaseInterface):
    scheme = "mysql"

    def __init__(
        self,
        dsn: Optional[str] = None,
        host: Optional[int] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
    ) -> None:
        super().__init__(
            dsn=dsn,
            host=host,
            port=port,
            user=user,
            password=password,
            db=db,
        )
        self._commit: ContextVar[bool] = ContextVar("commit")
        self._commit.set(True)

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
        return self._yield_connection()

    @asynccontextmanager
    async def _yield_connection(self):
        existing = self._connection.get(None)
        if existing:
            yield existing
        else:
            async with self._pool.acquire() as conn:
                await conn.begin()
                yield conn
                if self._commit.get(None):
                    await conn.commit()
                else:
                    await conn.rollback()
                    self._commit.set(True)
