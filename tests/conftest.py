from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from mayim import Mayim, PostgresExecutor, query
from mayim.base import Executor
from mayim.registry import (
    InterfaceRegistry,
    LazyHydratorRegistry,
    LazyQueryRegistry,
    Registry,
)
from mayim.sql.postgres import interface

from .app.model import Foo


class PostgresConnectionMock:
    def __init__(self):
        self.result = {"foo": "bar"}

    async def fetchall(self):
        return self.result

    async def fetchone(self):
        return self.result

    async def __aenter__(self, *args, **kwargs):
        return self

    async def __aexit__(self, *args, **kwargs):
        pass


@pytest.fixture(autouse=True)
def reset_registry():
    Registry().reset()
    InterfaceRegistry().reset()
    LazyQueryRegistry().reset()
    LazyHydratorRegistry().reset()


@pytest.fixture
def postgres_connection():
    connection = PostgresConnectionMock()
    connection.execute = AsyncMock(return_value=connection)
    connection.rollback = AsyncMock()
    connection.fetchone = AsyncMock(
        side_effect=lambda: getattr(connection, "result")
    )
    return connection


@pytest.fixture
def postgres_connection_context(postgres_connection):
    return Mock(return_value=postgres_connection)


@pytest.fixture(autouse=True)
def mock_postgres_pool(monkeypatch, postgres_connection_context):
    pool = AsyncMock()
    mock = MagicMock(return_value=pool)
    pool.connection = postgres_connection_context
    monkeypatch.setattr(interface, "AsyncConnectionPool", mock)
    return mock


@pytest.fixture
def FooExecutor():
    class FooExecutor(Executor):
        async def select_something(self) -> Foo:
            ...

        @classmethod
        def _load(cls, _):
            cls._loaded = True

    return FooExecutor


@pytest.fixture
def Item():
    @dataclass
    class Item:
        item_id: int
        name: str

    return Item


@pytest.fixture
def ItemExecutor(Item):
    single_query = "SELECT * FROM otheritems WHERE item_id=$item_id"
    single_query_positional = "SELECT * FROM otheritems WHERE item_id=$1"
    multiple_query = "SELECT * FROM otheritems"
    update_query = "UPDATE otheritems SET name=$name WHERE item_id=$item_id"

    class ItemExecutor(PostgresExecutor):
        @query(single_query)
        async def select_otheritem(self, item_id: int) -> Item:
            ...

        async def select_otheritem_execute(self, item_id: int) -> Item:
            return await self.execute(
                query=single_query, params={"item_id": item_id}
            )

        async def select_otheritem_run_sql(self, item_id: int):
            return await self.run_sql(
                query=single_query, params={"item_id": item_id}
            )

        async def select_item(self, item_id: int):
            return await self.run_sql(params={"item_id": item_id})

        async def select_item_named(self, item_id: int):
            return await self.run_sql(
                name="select_item",
                params={"item_id": item_id},
            )

        @query(single_query)
        async def select_int(self, item_id: int) -> int:
            ...

        async def select_int_execute(self, item_id: int) -> int:
            return await self.execute(
                query=single_query, params={"item_id": item_id}
            )

        @query(multiple_query)
        async def select_otheritems(self) -> List[Item]:
            ...

        async def select_otheritems_execute(self) -> List[Item]:
            return await self.execute(query=multiple_query, as_list=True)

        @query(single_query)
        async def select_optional_item(self, item_id: int) -> Optional[Item]:
            ...

        async def select_optional_item_execute(
            self, item_id: int
        ) -> Optional[Item]:
            return await self.execute(
                query=single_query,
                params={"item_id": item_id},
                allow_none=True,
            )

        @query(multiple_query)
        async def select_optional_items(self) -> Optional[List[Item]]:
            ...

        async def select_optional_items_execute(self) -> Optional[List[Item]]:
            return await self.execute(
                query=multiple_query, as_list=True, allow_none=True
            )

        @query(update_query)
        async def update_item_empty(self, item_id: int, name: str):
            ...

        @query(update_query)
        async def update_item_none(self, item_id: int, name: str) -> None:
            ...

        async def update_item_empty_execute(self, item_id: int, name: str):
            await self.execute(
                query=update_query, params={"item_id": item_id, "name": name}
            )

        async def update_item_none_execute(
            self, item_id: int, name: str
        ) -> None:
            await self.execute(
                query=update_query, params={"item_id": item_id, "name": name}
            )

        @query(single_query_positional)
        async def select_otheritem_positional(self, item_id: int) -> Item:
            ...

        async def select_otheritem_positional_execute(
            self, item_id: int
        ) -> Item:
            return await self.execute(
                query=single_query_positional, posargs=(item_id,)
            )

    return ItemExecutor


@pytest.fixture
async def item_executor(ItemExecutor):
    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    return Mayim.get(ItemExecutor)
