from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from mayim.sql.mysql.query import MysqlQuery

from ..executor import SQLExecutor

try:
    from asyncmy.cursors import DictCursor

    MYSQL_ENABLED = True
except ModuleNotFoundError:
    MYSQL_ENABLED = False


class MysqlExecutor(SQLExecutor):
    """Executor for interfacing with a MySQL database"""

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
