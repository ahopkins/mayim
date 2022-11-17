from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from mayim.sql.sqlserver.query import SQLServerQuery
from ..executor import SQLExecutor

try:
    import pyodbc  # noqa

    SQLSERVER_ENABLED = True
except ModuleNotFoundError:
    SQLSERVER_ENABLED = False


class SQLServerExecutor(SQLExecutor):
    """Executor for interfacing with a SQL Server database"""

    ENABLED = SQLSERVER_ENABLED
    QUERY_CLASS = SQLServerQuery
    POSITIONAL_SUB = r"?"
    KEYWORD_SUB = r":\2"

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
            cursor = conn.execute(query, exec_values or [])

            columns = [column[0] for column in cursor.description]
            if no_result:
                return None

            raw = getattr(cursor, method_name)()

            if not as_list:
                return dict(zip(columns, raw))

            results = []
            for row in raw:
                results.append(dict(zip(columns, row)))

            return results
