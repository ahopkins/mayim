from __future__ import annotations

from inspect import isawaitable
from typing import Any, Optional, Sequence, Type

from mayim.exception import RecordNotFound

from .sql import SQLExecutor

try:
    from asyncmy.cursors import DictCursor

    MYSQL_ENABLED = True
except ModuleNotFoundError:
    MYSQL_ENABLED = False


class MysqlExecutor(SQLExecutor):
    ENABLED = MYSQL_ENABLED

    async def _execute(
        self,
        query: str,
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        **values,
    ):
        if model is None:
            model, _ = self._context.get()
        factory = self.hydrator._make(model)
        raw = await self._run_sql(
            query=query, as_list=as_list, posargs=posargs, **values
        )
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
        posargs: Optional[Sequence[Any]] = None,
        **values,
    ):
        method_name = self._get_method(as_list=as_list)
        async with self.pool.connection() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(query)
                raw = await getattr(cursor, method_name)()
                return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"
