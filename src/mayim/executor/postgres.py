from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Coroutine, Dict, Optional, Sequence

from mayim.exception import MayimError
from mayim.query.postgres import PostgresQuery

from .sql import SQLExecutor

try:
    from psycopg.rows import dict_row

    POSTGRES_ENABLED = True
except ModuleNotFoundError:
    POSTGRES_ENABLED = False


class PostgresExecutor(SQLExecutor):
    ENABLED = POSTGRES_ENABLED
    QUERY_CLASS = PostgresQuery

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
            exec_values = list(posargs) if posargs else params
            cursor = await conn.execute(query, exec_values)
            if no_result:
                return None
            cursor.row_factory = dict_row
            raw = await getattr(cursor, method_name)()
            return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"

    @asynccontextmanager
    async def transaction(self):
        async with self.pool.connection() as conn:
            self.pool._connection.set(conn)
            yield conn.transaction()
            self.pool._connection.set(None)

    def rollback(self) -> Coroutine[Any, Any, Any]:
        existing = self.pool._connection.get(None)
        if not existing:
            raise MayimError("Cannot rollback non-existing transaction")
        return existing.rollback()
