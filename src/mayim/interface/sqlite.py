from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

from mayim.exception import MayimError
from mayim.interface.base import BaseInterface

try:
    import aiosqlite

    AIOSQLITE_ENABLED = True
except ModuleNotFoundError:
    AIOSQLITE_ENABLED = False


class SQLitePool(BaseInterface):
    scheme = "sqlite"

    def __init__(self, db_path: str):
        self._db_path = db_path

    def _setup_pool(self):
        if not AIOSQLITE_ENABLED:
            raise MayimError(
                "SQLite driver not found. Try reinstalling Mayim: "
                "pip install mayim[sqlite]"
            )

    async def open(self):
        ...
        # self._db = await aiosqlite.connect(self._db_path)
        # self._db.row_factory = aiosqlite.Row

    async def close(self):
        ...
        # await self._db.close()

    @asynccontextmanager
    async def connection(self, timeout: Optional[float] = None):
        yield self._db
        # ) -> AsyncIterator[Connection]:
        # existing = self.existing_connection()
        # if existing:
        #     yield existing
        # else:
        #     transaction = self.in_transaction()
        #     async with self._pool.acquire() as conn:
        #         if transaction:
        #             await conn.begin()
        #         yield conn
        #         if transaction:
        #             if self.do_commit():
        #                 await conn.commit()
