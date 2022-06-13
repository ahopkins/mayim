from __future__ import annotations

from inspect import isawaitable
from typing import Any, Dict, Optional, Sequence, Type

from mayim.exception import RecordNotFound
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

    async def _execute(
        self,
        query: str,
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        keyargs: Optional[Dict[str, Any]] = None,
    ):
        no_result = False
        if model is None:
            model, _ = self._context.get()
        if model is None:
            no_result = True
        factory = self.hydrator._make(model)
        raw = await self._run_sql(
            query=query,
            name=name,
            as_list=as_list,
            no_result=no_result,
            posargs=posargs,
            keyargs=keyargs,
        )
        if no_result:
            return None
        if not raw:
            raise RecordNotFound("not found")
        results = factory(raw)
        if isawaitable(results):
            results = await results
        return results

    async def _run_sql(
        self,
        query: str,
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        keyargs: Optional[Dict[str, Any]] = None,
    ):
        method_name = self._get_method(as_list=as_list)
        async with self.pool.connection() as conn:
            exec_values = list(posargs) if posargs else keyargs
            cursor = await conn.execute(query, exec_values)
            if no_result:
                return None
            cursor.row_factory = dict_row
            raw = await getattr(cursor, method_name)()
            return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"
