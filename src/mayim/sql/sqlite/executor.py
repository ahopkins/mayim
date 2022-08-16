from __future__ import annotations

from sqlite3 import Cursor
from typing import Any, Dict, Optional, Sequence, Tuple

from mayim.sql.sqlite.query import SQLiteQuery

from ..executor import SQLExecutor

try:
    import aiosqlite  # noqa

    AIOSQLITE_ENABLED = True
except ModuleNotFoundError:
    AIOSQLITE_ENABLED = False


class SQLiteExecutor(SQLExecutor):
    """Executor for interfacing with a SQLite database"""

    ENABLED = AIOSQLITE_ENABLED
    QUERY_CLASS = SQLiteQuery
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
            conn.row_factory = self._dict_factory
            cursor = await conn.execute(query, exec_values)
            if no_result:
                return None
            raw = await getattr(cursor, method_name)()
            return raw

    @staticmethod
    def _dict_factory(cursor: Cursor, row: Tuple[Any, ...]) -> Dict[str, Any]:
        return {val[0]: row[idx] for idx, val in enumerate(cursor.description)}
