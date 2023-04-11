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
    """Interface for connecting to a SQLite database"""

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
        """Open connections to the pool"""
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row

    async def close(self):
        """Close connections to the pool"""
        await self._db.close()

    @asynccontextmanager
    async def connection(self, timeout: Optional[float] = None):
        """Obtain a connection to the database

        Args:
            timeout (float, optional): _Not implemented_. Defaults to `None`.

        Returns:
            AsyncIterator[Connection]: Iterator that will yield a connection

        Yields:
            Iterator[AsyncIterator[Connection]]: A database connection
        """
        existing = self.existing_connection()
        close_when_done = False

        if existing:
            yield existing
        else:
            if not self._db:
                close_when_done = True
                await self.open()
            yield self._db

        transaction = self.in_transaction()
        commit = self.do_commit()

        if not transaction:
            if commit:
                await self._db.commit()  # type: ignore

        if close_when_done:
            await self.close()
