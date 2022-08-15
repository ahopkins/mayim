from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

try:
    from asyncmy import Connection, create_pool

    MYSQL_ENABLED = True
except ModuleNotFoundError:
    MYSQL_ENABLED = False
    Connection = type("Connection", (), {})  # type: ignore


class MysqlPool(BaseInterface):
    scheme = "mysql"

    def _setup_pool(self):
        if not MYSQL_ENABLED:
            raise MayimError(
                "MySQL driver not found. Try reinstalling Mayim: "
                "pip install mayim[mysql]"
            )
        self._pool = create_pool(
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

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[Connection]:
        existing = self.existing_connection()
        if existing:
            yield existing
        else:
            transaction = self.in_transaction()
            async with self._pool.acquire() as conn:
                if transaction:
                    await conn.begin()
                yield conn
                if transaction:
                    if self.do_commit():
                        await conn.commit()
