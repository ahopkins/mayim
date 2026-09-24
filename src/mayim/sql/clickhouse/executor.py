from __future__ import annotations

from typing import Any, Dict, Optional, Sequence

from mayim.sql.clickhouse.query import ClickhouseQuery

from ..executor import SQLExecutor
from .interface import CLICKHOUSE_ENABLED


class ClickhouseExecutor(SQLExecutor):
    """Executor for interfacing with a ClickHouse database

    Queries are bound client side using pyformat parameters, so literal
    `%` characters in queries that take parameters must be escaped as
    `%%`. Methods that should not return a result (such as `INSERT` or
    DDL statements) should be annotated with `-> None` so that they are
    routed through the driver's `command` method.
    """

    ENABLED = CLICKHOUSE_ENABLED
    QUERY_CLASS = ClickhouseQuery

    async def _run_sql(
        self,
        query: str,
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        async with self.pool.connection() as client:
            exec_values = list(posargs) if posargs else params
            if no_result:
                await client.command(query, parameters=exec_values)
                return None
            result = await client.query(query, parameters=exec_values)
            raw = [
                dict(zip(result.column_names, row))
                for row in result.result_rows
            ]
            if as_list:
                return raw
            return raw[0] if raw else None
