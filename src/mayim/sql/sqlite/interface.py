from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

try:
    import aiosqlite

    AIOSQLITE_ENABLED = True
except ModuleNotFoundError:
    AIOSQLITE_ENABLED = False


class SQLitePool(BaseInterface):
    scheme = ""

    def __init__(self, db_path: str):
        self._db_path = db_path
        super().__init__()

    def _setup_pool(self):
        if not AIOSQLITE_ENABLED:
            raise MayimError(
                "SQLite driver not found. Try reinstalling Mayim: "
                "pip install mayim[sqlite]"
            )

    async def open(self):
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row

    async def close(self):
        await self._db.close()

    @asynccontextmanager
    async def connection(self, timeout: Optional[float] = None):
        close_when_done = False
        if not self._db:
            close_when_done = True
            await self.open()

        yield self._db

        if close_when_done:
            await self.close()
