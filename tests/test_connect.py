import pytest

from mayim import Mayim
from mayim.impl.sql.postgres.interface import PostgresPool
from mayim.lazy.interface import LazyPool


@pytest.fixture
def mayim(FooExecutor):
    return Mayim(
        executors=[FooExecutor], dsn="postgres://user:password@host:1234/db"
    )


async def test_connect_derived_pool(FooExecutor, mayim):
    assert isinstance(FooExecutor._fallback_pool, LazyPool)
    await mayim.connect()
    assert isinstance(FooExecutor._fallback_pool, PostgresPool)

    executor = FooExecutor()
    assert isinstance(executor.pool, PostgresPool)
