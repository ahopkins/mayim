from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from mayim.executor import Executor
from mayim.interface import postgres
from mayim.registry import (
    InterfaceRegistry,
    LazyHydratorRegistry,
    LazySQLRegistry,
    Registry,
)

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
    LazySQLRegistry().reset()
    LazyHydratorRegistry().reset()


@pytest.fixture
def postgres_connection():
    connection = PostgresConnectionMock()
    connection.execute = AsyncMock(return_value=connection)
    connection.fetchone = AsyncMock(
        side_effect=lambda: getattr(connection, "result")
    )
    return connection


@pytest.fixture
def postgres_connection_context(postgres_connection):
    return Mock(return_value=postgres_connection)


@pytest.fixture(autouse=True)
def mock_postgres_pool(monkeypatch, postgres_connection_context):
    pool = Mock()
    mock = MagicMock(return_value=pool)
    pool.connection = postgres_connection_context
    monkeypatch.setattr(postgres, "AsyncConnectionPool", mock)
    return mock


@pytest.fixture
def FooExecutor():
    class FooExecutor(Executor):
        async def select_something(self) -> Foo:
            ...

        @classmethod
        def _load(cls):
            cls._loaded = True

    return FooExecutor
