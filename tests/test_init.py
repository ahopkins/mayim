from unittest.mock import AsyncMock

from mayim import Mayim, register
from mayim.base.executor import Executor
from mayim.registry import Registry
from mayim.sql.mysql.interface import MysqlPool
from mayim.sql.postgres.interface import PostgresPool


def test_universal_pool_dsn(FooExecutor):
    Mayim(executors=[FooExecutor], dsn="foo://user:password@host:1234/db")

    assert len(Registry()) == 1
    assert FooExecutor._loaded

    executor = Mayim.get(FooExecutor)
    assert executor.pool


def test_universal_pool_object(FooExecutor):
    pool = AsyncMock()
    Mayim(executors=[FooExecutor], pool=pool)

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == pool

    executor = Mayim.get(FooExecutor)
    assert executor.pool == pool


def test_universal_pool_object_register(FooExecutor):
    register(FooExecutor)
    pool = AsyncMock()
    Mayim(pool=pool)

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == pool

    executor = Mayim.get(FooExecutor)
    assert executor.pool == pool


def test_executor_pool_object_load_class(FooExecutor):
    pool = AsyncMock()
    fallback_pool = AsyncMock()
    executor = FooExecutor(pool=pool)
    Mayim(executors=[FooExecutor], pool=fallback_pool)

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == fallback_pool
    assert executor.pool == pool
    assert executor.pool != fallback_pool


def test_executor_pool_object_load_object(FooExecutor):
    pool = AsyncMock()
    fallback_pool = AsyncMock()
    executor = FooExecutor(pool=pool)
    Mayim(executors=[executor], pool=fallback_pool)

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == fallback_pool
    assert executor.pool == pool
    assert executor.pool != fallback_pool


def test_executor_pool_object_instantiated(FooExecutor):
    pool = AsyncMock()
    fallback_pool = AsyncMock()
    executor = FooExecutor(pool=pool)
    Mayim(pool=fallback_pool)

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == fallback_pool
    assert executor.pool == pool
    assert executor.pool != fallback_pool


def test_universal_pool_instance_load_class(FooExecutor):
    pool = AsyncMock()
    mayim = Mayim(pool=pool)
    mayim.load(executors=[FooExecutor])

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == pool

    executor = Mayim.get(FooExecutor)
    assert executor.pool == pool


def test_universal_pool_instance_load_object(FooExecutor):
    pool = AsyncMock()
    mayim = Mayim(pool=pool)
    executor = FooExecutor()
    mayim.load(executors=[executor])

    assert len(Registry()) == 1
    assert FooExecutor._loaded
    assert FooExecutor._fallback_pool == pool
    assert executor.pool == pool


def test_over_load(FooExecutor):
    register(FooExecutor)
    pool = AsyncMock()
    mayim = Mayim(pool=pool)
    executor = FooExecutor()
    mayim.load(executors=[executor, FooExecutor])

    assert len(Registry()) == 1


def test_register(FooExecutor):
    cls = register(FooExecutor)
    assert cls is FooExecutor
    assert len(Registry()) == 1


def test_fallback_to_postgres(FooExecutor):
    Mayim(dsn="postgres://user:password@host:1234/db")
    assert Executor._fallback_pool._derivative is PostgresPool


def test_fallback_to_mysql(FooExecutor):
    Mayim(dsn="mysql://user:password@host:1234/db")
    assert Executor._fallback_pool._derivative is MysqlPool
