from __future__ import annotations
from contextlib import asynccontextmanager

from typing import Any, Dict, Optional, Sequence
from mayim.exception import MayimError

from mayim.query.mysql import MysqlQuery

from .sql import SQLExecutor

try:
    from asyncmy.cursors import DictCursor

    MYSQL_ENABLED = True
except ModuleNotFoundError:
    MYSQL_ENABLED = False


class MysqlExecutor(SQLExecutor):
    ENABLED = MYSQL_ENABLED
    QUERY_CLASS = MysqlQuery

    async def _run_sql(
        self,
        query: str,
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        method_name = self._get_method(as_list=as_list)
        async with self.pool.connection() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                exec_values = list(posargs) if posargs else params
                await cursor.execute(query, exec_values)
                if no_result:
                    return None
                raw = await getattr(cursor, method_name)()
                return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.connection() as conn:
            self.pool._connection.set(conn)
            yield conn
            self.pool._connection.set(None)

    async def rollback(self) -> None:
        existing = self.pool._connection.get(None)
        if not existing:
            raise MayimError("Cannot rollback non-existing transaction")
        self.pool._commit.set(False)  # type: ignore
