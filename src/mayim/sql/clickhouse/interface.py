from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, Optional
from urllib.parse import parse_qsl

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

try:
    # The driver itself raises RuntimeError at import time on Python
    # versions it does not support, so catch that in addition to a
    # missing installation
    from clickhouse_connect import get_async_client

    CLICKHOUSE_ENABLED = True
except (ImportError, RuntimeError):
    CLICKHOUSE_ENABLED = False

    async def get_async_client(*args, **kwargs):  # type: ignore
        return None


DEFAULT_HTTP_PORT = 8123
DEFAULT_CONNECTOR_LIMIT = 100


class ClickhousePool(BaseInterface):
    """Interface for connecting to a ClickHouse database.

    Unlike the other SQL interfaces, the ClickHouse driver is HTTP based
    and manages its own connection pooling internally. A single shared
    client is therefore created and yielded for every request, with the
    `max_size` of the pool mapping onto the driver's connector limit.

    There are two distinct ways to push configuration at ClickHouse, and
    they are easy to confuse:

    - **Query settings** (this class' `settings`) are server side
      execution controls such as `readonly`, `max_execution_time` or
      `max_result_rows`. They are handed to the driver once, when the
      shared client is created, and therefore apply to every query made
      through the pool for the lifetime of that client. Reach for these
      when you want static caps, for example a service that should only
      ever run bounded, read-only queries regardless of what the
      ClickHouse user has been granted.
    - **Transport settings**
      (`mayim.sql.clickhouse.executor.ClickhouseExecutor.transport_settings`)
      are HTTP headers attached to a single request. That hook is
      re-evaluated for every query and command, so it is the one to
      override for request scoped context such as a `traceparent` or a
      request ID pulled from a `ContextVar`.

    Query settings may be passed as a `settings` mapping, as DSN query
    parameters, or both:

    ```python
    ClickhousePool(
        "clickhouse://user:password@localhost:8123/default",
        settings={"readonly": 1, "max_execution_time": 30},
    )
    ClickhousePool(
        "clickhouse://user:password@localhost:8123/default"
        "?readonly=1&max_execution_time=30"
    )
    ```

    Every DSN query parameter is treated as a ClickHouse setting, since
    this interface has no other use for the query string. Values parsed
    from a DSN are always strings (`readonly="1"`), which is what the
    HTTP protocol transmits anyway. Where a key is supplied both ways,
    the `settings` mapping wins.

    Args:
        settings (Dict[str, Any], optional): ClickHouse query settings
            applied to every query made through this pool. Merged over
            any settings parsed from the DSN query string. Defaults to
            `None`.

    Attributes:
        supports_transactions (bool): Always `False`. ClickHouse does not
            support the interactive transactions that the transaction
            coordinator relies upon.
    """

    scheme = "clickhouse"
    supports_transactions = False

    def __init__(
        self,
        *args,
        settings: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> None:
        # Assigned before the base class runs, since its __init__ calls
        # _setup_pool() inline
        self._settings_override = dict(settings) if settings else {}
        super().__init__(*args, **kwargs)

    def _setup_pool(self):
        if not CLICKHOUSE_ENABLED:
            raise MayimError(
                "ClickHouse driver not found. Try reinstalling Mayim: "
                "pip install mayim[clickhouse] (requires Python 3.10+)"
            )
        self._settings: Dict[str, Any] = {
            **self._settings_from_query(),
            **self._settings_override,
        }
        self._client = None
        self._client_lock = asyncio.Lock()

    def _settings_from_query(self) -> Dict[str, Any]:
        return dict(parse_qsl(self._query)) if self._query else {}

    @property
    def settings(self) -> Dict[str, Any]:
        return self._settings

    async def _get_client(self):
        if self._client is None:
            # The driver's factory is a coroutine, so the client is
            # created on first use rather than at pool construction
            async with self._client_lock:
                if self._client is None:
                    self._client = await get_async_client(
                        host=self.host,
                        port=self.port or DEFAULT_HTTP_PORT,
                        username=self.user,
                        password=self.password,
                        database=self.db or None,
                        connector_limit=(
                            self.max_size or DEFAULT_CONNECTOR_LIMIT
                        ),
                        settings=self._settings,
                    )
        return self._client

    async def open(self):
        """Create the shared client (if needed) and ping it.

        This surfaces connection problems at `Mayim.connect()` time
        rather than at first query.
        """
        client = await self._get_client()
        await client.ping()

    async def close(self):
        """Close the shared client and its underlying HTTP connections"""
        if self._client is not None:
            await self._client.close()
            self._client = None

    @asynccontextmanager
    async def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncIterator[Any]:
        """Obtain the shared ClickHouse client

        The driver multiplexes queries over its own pooled HTTP
        connections, so the same client instance is yielded every time.

        Args:
            timeout (float, optional): _Not implemented_. The driver
                manages its own connect and receive timeouts.
                Defaults to `None`.

        Yields:
            The shared `clickhouse_connect` async client
        """
        existing = self.existing_connection()
        if existing:
            yield existing
        else:
            yield await self._get_client()
