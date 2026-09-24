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

    def transport_settings(self) -> Optional[Dict[str, str]]:
        """Transport level settings to send as HTTP headers on each call.

        The value is evaluated fresh for every query and command, so it
        can be derived from per-task context (such as a `ContextVar`)
        even though the ClickHouse client is shared across concurrent
        requests. The driver copies the returned mapping into the
        headers of that single request only; the client itself is never
        modified.

        Override this method to attach headers such as a W3C
        `traceparent` or a request ID:

        ```python
        from contextvars import ContextVar

        traceparent: ContextVar[Optional[str]] = ContextVar(
            "traceparent", default=None
        )


        class ItemExecutor(ClickhouseExecutor):
            async def select_item(self, item_id: int) -> Item: ...

            def transport_settings(self) -> Optional[Dict[str, str]]:
                value = traceparent.get()
                if value is None:
                    return None
                return {"traceparent": value}
        ```

        Returns:
            A mapping of HTTP header names to values, or `None` (the
            default) to send no extra headers.
        """
        return None

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
            transport_settings = self.transport_settings()
            if no_result:
                await client.command(
                    query,
                    parameters=exec_values,
                    transport_settings=transport_settings,
                )
                return None
            result = await client.query(
                query,
                parameters=exec_values,
                transport_settings=transport_settings,
            )
            raw = [
                dict(zip(result.column_names, row))
                for row in result.result_rows
            ]
            if as_list:
                return raw
            return raw[0] if raw else None
