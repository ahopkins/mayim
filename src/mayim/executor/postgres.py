from __future__ import annotations


from inspect import isawaitable
from typing import Optional, Type


from mayim.exception import RecordNotFound
from .sql import SQLExecutor

try:
    from psycopg.rows import dict_row

    POSTGRES_ENABLED = True
except ModuleNotFoundError:
    POSTGRES_ENABLED = False


class PostgresExecutor(SQLExecutor):
    ENABLED = POSTGRES_ENABLED

    async def _execute(
        self,
        query: str,
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        **values,
    ):
        if model is None:
            model, _ = self._context.get()
        factory = self.hydrator._make(model)
        raw = await self._run_sql(query=query, as_list=as_list, **values)
        if not raw:
            raise RecordNotFound("not found")
        results = factory(raw)
        if isawaitable(results):
            results = await results
        return results

    async def _run_sql(
        self,
        query: str,
        as_list: bool = False,
        **values,
    ):
        method_name = self._get_method(as_list=as_list)
        async with self.pool.connection() as conn:
            cursor = await conn.execute(query, values)
            cursor.row_factory = dict_row
            raw = await getattr(cursor, method_name)()
            return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"
