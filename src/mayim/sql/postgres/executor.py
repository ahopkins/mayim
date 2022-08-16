from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from mayim.sql.postgres.query import PostgresQuery

from ..executor import SQLExecutor

try:
    from psycopg.rows import dict_row

    POSTGRES_ENABLED = True
except ModuleNotFoundError:
    POSTGRES_ENABLED = False


class PostgresExecutor(SQLExecutor):
    """Executor for interfacing with a Postgres database"""

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
