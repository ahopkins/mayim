from contextvars import ContextVar
from dataclasses import asdict, dataclass
from types import SimpleNamespace
from typing import Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from mayim import ClickhouseExecutor, Mayim, query
from mayim.exception import MayimError, RecordNotFound
from mayim.sql.clickhouse import interface
from mayim.sql.clickhouse.interface import ClickhousePool

DSN = "clickhouse://default:password@localhost:8123/default"

request_id: ContextVar[Optional[str]] = ContextVar("request_id", default=None)


@pytest.fixture
def clickhouse_client():
    client = MagicMock()
    client.query = AsyncMock()
    client.command = AsyncMock()
    client.ping = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_clickhouse_pool(monkeypatch, clickhouse_client):
    factory = AsyncMock(return_value=clickhouse_client)
    monkeypatch.setattr(interface, "get_async_client", factory)
    monkeypatch.setattr(interface, "CLICKHOUSE_ENABLED", True)
    monkeypatch.setattr(ClickhouseExecutor, "ENABLED", True)
    return factory


@dataclass
class Item:
    item_id: int
    name: str


@pytest.fixture
def ClickhouseItemExecutor():
    single_query = "SELECT * FROM items WHERE item_id=$item_id"
    single_query_positional = "SELECT * FROM items WHERE item_id=$1"
    multiple_query = "SELECT * FROM items"
    insert_query = "INSERT INTO items VALUES ($item_id, $name)"

    class ClickhouseItemExecutor(ClickhouseExecutor):
        @query(single_query)
        async def select_item(self, item_id: int) -> Item: ...

        @query(single_query)
        async def select_optional_item(
            self, item_id: int
        ) -> Optional[Item]: ...

        @query(single_query_positional)
        async def select_item_positional(self, item_id: int) -> Item: ...

        @query(multiple_query)
        async def select_items(self) -> List[Item]: ...

        @query(insert_query)
        async def insert_item(self, item_id: int, name: str) -> None: ...

    return ClickhouseItemExecutor


@pytest.fixture
async def clickhouse_executor(mock_clickhouse_pool, ClickhouseItemExecutor):
    Mayim(executors=[ClickhouseItemExecutor], dsn=DSN)
    return Mayim.get(ClickhouseItemExecutor)


@pytest.fixture
def TracedItemExecutor():
    single_query = "SELECT * FROM items WHERE item_id=$item_id"
    insert_query = "INSERT INTO items VALUES ($item_id, $name)"

    class TracedItemExecutor(ClickhouseExecutor):
        @query(single_query)
        async def select_item(self, item_id: int) -> Item: ...

        @query(insert_query)
        async def insert_item(self, item_id: int, name: str) -> None: ...

        def transport_settings(self) -> Optional[Dict[str, str]]:
            value = request_id.get()
            if value is None:
                return None
            return {"x-request-id": value}

    return TracedItemExecutor


@pytest.fixture
async def traced_executor(mock_clickhouse_pool, TracedItemExecutor):
    Mayim(executors=[TracedItemExecutor], dsn=DSN)
    return Mayim.get(TracedItemExecutor)


async def test_returns_single_item(clickhouse_client, clickhouse_executor):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar")],
        column_names=("item_id", "name"),
    )
    result = await clickhouse_executor.select_item(item_id=999)

    clickhouse_client.query.assert_called_with(
        "SELECT * FROM items WHERE item_id=%(item_id)s",
        parameters={"item_id": 999},
        transport_settings=None,
    )
    assert isinstance(result, Item)
    assert asdict(result) == {"item_id": 999, "name": "FooBar"}


async def test_returns_single_item_positional(
    clickhouse_client, clickhouse_executor
):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar")],
        column_names=("item_id", "name"),
    )
    result = await clickhouse_executor.select_item_positional(item_id=999)

    clickhouse_client.query.assert_called_with(
        "SELECT * FROM items WHERE item_id=%s",
        parameters=[999],
        transport_settings=None,
    )
    assert isinstance(result, Item)
    assert asdict(result) == {"item_id": 999, "name": "FooBar"}


async def test_returns_multiple_items(clickhouse_client, clickhouse_executor):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar"), (1000, "BarFoo")],
        column_names=("item_id", "name"),
    )
    results = await clickhouse_executor.select_items()

    clickhouse_client.query.assert_called_with(
        "SELECT * FROM items", parameters=None, transport_settings=None
    )
    assert [asdict(result) for result in results] == [
        {"item_id": 999, "name": "FooBar"},
        {"item_id": 1000, "name": "BarFoo"},
    ]


async def test_empty_result_as_list(clickhouse_client, clickhouse_executor):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[],
        column_names=("item_id", "name"),
    )
    assert await clickhouse_executor.select_items() == []


async def test_empty_result_raises(clickhouse_client, clickhouse_executor):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[],
        column_names=("item_id", "name"),
    )
    with pytest.raises(RecordNotFound):
        await clickhouse_executor.select_item(item_id=999)


async def test_empty_result_optional_item(
    clickhouse_client, clickhouse_executor
):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[],
        column_names=("item_id", "name"),
    )
    result = await clickhouse_executor.select_optional_item(item_id=999)
    assert result is None


async def test_no_result_uses_command(clickhouse_client, clickhouse_executor):
    result = await clickhouse_executor.insert_item(item_id=1, name="Foo")

    clickhouse_client.command.assert_called_with(
        "INSERT INTO items VALUES (%(item_id)s, %(name)s)",
        parameters={"item_id": 1, "name": "Foo"},
        transport_settings=None,
    )
    clickhouse_client.query.assert_not_called()
    assert result is None


async def test_transport_settings_none_without_context(
    clickhouse_client, traced_executor
):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar")],
        column_names=("item_id", "name"),
    )
    await traced_executor.select_item(item_id=999)

    clickhouse_client.query.assert_called_with(
        "SELECT * FROM items WHERE item_id=%(item_id)s",
        parameters={"item_id": 999},
        transport_settings=None,
    )


async def test_transport_settings_forwarded_to_query(
    clickhouse_client, traced_executor
):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar")],
        column_names=("item_id", "name"),
    )
    token = request_id.set("req-1")
    try:
        await traced_executor.select_item(item_id=999)
    finally:
        request_id.reset(token)

    clickhouse_client.query.assert_called_with(
        "SELECT * FROM items WHERE item_id=%(item_id)s",
        parameters={"item_id": 999},
        transport_settings={"x-request-id": "req-1"},
    )


async def test_transport_settings_forwarded_to_command(
    clickhouse_client, traced_executor
):
    token = request_id.set("req-2")
    try:
        result = await traced_executor.insert_item(item_id=1, name="Foo")
    finally:
        request_id.reset(token)

    clickhouse_client.command.assert_called_with(
        "INSERT INTO items VALUES (%(item_id)s, %(name)s)",
        parameters={"item_id": 1, "name": "Foo"},
        transport_settings={"x-request-id": "req-2"},
    )
    clickhouse_client.query.assert_not_called()
    assert result is None


async def test_transport_settings_evaluated_per_call(
    clickhouse_client, traced_executor
):
    clickhouse_client.query.return_value = SimpleNamespace(
        result_rows=[(999, "FooBar")],
        column_names=("item_id", "name"),
    )
    token = request_id.set("req-1")
    try:
        await traced_executor.select_item(item_id=999)
        request_id.set("req-2")
        await traced_executor.select_item(item_id=999)
        await traced_executor.insert_item(item_id=1, name="Foo")
    finally:
        request_id.reset(token)

    assert clickhouse_client.query.call_args_list == [
        call(
            "SELECT * FROM items WHERE item_id=%(item_id)s",
            parameters={"item_id": 999},
            transport_settings={"x-request-id": "req-1"},
        ),
        call(
            "SELECT * FROM items WHERE item_id=%(item_id)s",
            parameters={"item_id": 999},
            transport_settings={"x-request-id": "req-2"},
        ),
    ]
    clickhouse_client.command.assert_called_with(
        "INSERT INTO items VALUES (%(item_id)s, %(name)s)",
        parameters={"item_id": 1, "name": "Foo"},
        transport_settings={"x-request-id": "req-2"},
    )


async def test_transaction_not_supported(clickhouse_executor):
    with pytest.raises(MayimError, match="does not support transactions"):
        await clickhouse_executor.begin()


async def test_pool_requires_driver(monkeypatch):
    monkeypatch.setattr(interface, "CLICKHOUSE_ENABLED", False)
    with pytest.raises(MayimError, match=r"pip install mayim\[clickhouse\]"):
        ClickhousePool(DSN)


async def test_pool_passes_connection_args(mock_clickhouse_pool):
    pool = ClickhousePool(DSN)

    async with pool.connection() as client:
        assert client is mock_clickhouse_pool.return_value

    mock_clickhouse_pool.assert_awaited_with(
        host="localhost",
        port=8123,
        username="default",
        password="password",
        database="default",
        connector_limit=100,
    )


async def test_pool_defaults_http_port(mock_clickhouse_pool):
    pool = ClickhousePool("clickhouse://default@localhost/db")

    async with pool.connection():
        ...

    mock_clickhouse_pool.assert_awaited_with(
        host="localhost",
        port=8123,
        username="default",
        password=None,
        database="db",
        connector_limit=100,
    )


async def test_pool_open_and_close(mock_clickhouse_pool, clickhouse_client):
    pool = ClickhousePool(DSN)
    await pool.open()
    clickhouse_client.ping.assert_awaited()

    async with pool.connection() as client:
        assert client is clickhouse_client

    await pool.close()
    clickhouse_client.close.assert_awaited()
    assert pool._client is None
