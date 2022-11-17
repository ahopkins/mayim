from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import parse_qs, urlparse

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

try:
    import pyodbc

    SQLSERVER_ENABLED = True
except ModuleNotFoundError:
    SQLSERVER_ENABLED = False


class SQLServerPool(BaseInterface):
    """Interface for connecting to a SQL Server database"""

    scheme = "mssql+pyodbc"

    def __init__(self, db_path: str):
        self._db_path = db_path
        super().__init__()

    def _setup_pool(self):
        if not SQLSERVER_ENABLED:
            raise MayimError(
                "SQL Server driver not found. Try reinstalling Mayim: "
                "pip install mayim[sqlserver]"
            )

    def _parse_url(self) -> str:
        url = urlparse(self._db_path)
        query = parse_qs(url.query)
        driver = query.get("DRIVER", "")
        conn_string = f"DRIVER={driver};SERVER={url.hostname};PORT={url.port};DATABASE={url.path.strip('/')};UID={url.username};PWD={url.password}"
        return conn_string

    async def open(self):
        """Open connections to the pool"""
        conn_string = self._parse_url()
        self._db = pyodbc.connect(conn_string)

    async def close(self):
        """Close connections to the pool"""
        self._db.close()

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
        if not self._db:
            await self.open()

        yield self._db
