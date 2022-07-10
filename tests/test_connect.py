import pytest

from mayim import Mayim
from mayim.interface.lazy import LazyPool
from mayim.interface.postgres import PostgresPool


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
