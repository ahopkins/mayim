"""
Tests specifically for the new explicit transaction API.
These tests define the exact API we want to implement for explicit transaction control.
"""

from unittest.mock import AsyncMock, MagicMock
from dataclasses import dataclass
from typing import List

import pytest

from mayim import Mayim, PostgresExecutor, query
from mayim.exception import MayimError


@dataclass
class FoobarModel:
    id: int
    value: str


class FoobarExecutor(PostgresExecutor):
    @query("INSERT INTO test (value) VALUES ($value) RETURNING *")
    async def insert_test(self, value: str) -> FoobarModel: ...

    @query("UPDATE test SET value = $value WHERE id = $id")
    async def update_test(self, id: int, value: str) -> None: ...

    # Override _load to avoid needing SQL files for tests
    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


# ============================================================================
# NEW EXPLICIT TRANSACTION API SPECIFICATION
# ============================================================================


async def test_explicit_api_basic_usage(postgres_connection):
    """
    SPECIFICATION: Basic usage of explicit transaction API.

    The API should support:
    1. Creating a transaction object without starting it
    2. Explicitly beginning the transaction
    3. Performing operations
    4. Explicitly committing or rolling back
    """
    # Setup
    Mayim(executors=[FoobarExecutor], dsn="postgres://user:pass@localhost:5432/test")
    executor = Mayim.get(FoobarExecutor)

    # Create transaction object (doesn't start transaction yet)
    txn = await Mayim.transaction(FoobarExecutor)

    # Verify initial state
    assert not txn.is_active, "Transaction should not be active yet"
    assert not txn.is_committed
    assert not txn.is_rolled_back

    # Begin transaction
    await txn.begin()
    assert txn.is_active, "Transaction should be active after begin()"

    # Mock the operations since we don't have a real database
    postgres_connection.result = {"id": 1, "value": "test1"}
    result = await executor.insert_test(value="test1")
    # For testing purposes, just verify the transaction state
    assert txn.is_active, "Transaction should remain active during operations"

    # Commit transaction
    await txn.commit()
    assert not txn.is_active, "Transaction should not be active after commit"
    assert txn.is_committed, "Transaction should be marked as committed"


async def test_explicit_api_with_multiple_executors():
    """
    SPECIFICATION: Explicit API with multiple executors.

    Should support passing multiple executor classes or instances.
    """

    class AnotherExecutor(PostgresExecutor):
        @query("INSERT INTO another (data) VALUES ($data)")
        async def insert_another(self, data: str) -> None: ...

    Mayim(executors=[FoobarExecutor, AnotherExecutor], dsn="postgres://localhost/test")

    # Create transaction with multiple executors
    txn = await Mayim.transaction(FoobarExecutor, AnotherExecutor)
    await txn.begin()

    # Both executors should work within the transaction
    test_exec = Mayim.get(FoobarExecutor)
    another_exec = Mayim.get(AnotherExecutor)

    await test_exec.insert_test(value="test")
    await another_exec.insert_another(data="data")

    await txn.commit()
    assert txn.is_committed


async def test_explicit_api_rollback_behavior():
    """
    SPECIFICATION: Explicit rollback behavior.

    Should support explicit rollback at any point.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)
    await txn.begin()

    executor = Mayim.get(FoobarExecutor)
    await executor.insert_test(value="will_be_rolled_back")

    # Explicit rollback
    await txn.rollback()

    assert not txn.is_active
    assert txn.is_rolled_back
    assert not txn.is_committed


async def test_explicit_api_context_manager_compatibility(postgres_connection):
    """
    SPECIFICATION: Context manager compatibility.

    The transaction object should also work as a context manager.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://user:pass@localhost:5432/test")
    executor = Mayim.get(FoobarExecutor)

    # Method 1: Explicit API
    txn1 = await Mayim.transaction(FoobarExecutor)
    await txn1.begin()
    postgres_connection.result = {"id": 1, "value": "explicit"}
    await executor.insert_test(value="explicit")
    await txn1.commit()
    assert txn1.is_committed

    # Method 2: Context manager (auto-begins and auto-commits/rollbacks)
    txn2 = await Mayim.transaction(FoobarExecutor)
    async with txn2:
        postgres_connection.result = {"id": 2, "value": "context_manager"}
        await executor.insert_test(value="context_manager")
    assert txn2.is_committed

    # Method 3: Direct context manager (current style, should still work)
    txn3 = await Mayim.transaction(FoobarExecutor)
    async with txn3:
        postgres_connection.result = {"id": 3, "value": "direct_context"}
        await executor.insert_test(value="direct_context")
        # Can access transaction object if needed
        assert txn3.is_active
    assert txn3.is_committed


async def test_explicit_api_error_handling():
    """
    SPECIFICATION: Error handling in explicit API.

    Should handle errors gracefully and maintain correct state.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)

    # Error: Cannot commit before begin
    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.commit()

    # Error: Cannot rollback before begin
    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.rollback()

    await txn.begin()

    # Error: Cannot begin twice
    with pytest.raises(MayimError, match="Transaction already active"):
        await txn.begin()

    await txn.commit()

    # Error: Cannot commit after already committed
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.commit()

    # Error: Cannot rollback after commit
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.rollback()

    # Error: Cannot begin after completion
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.begin()


async def test_explicit_api_with_executor_instances():
    """
    SPECIFICATION: Support both executor classes and instances.

    Should accept both types when creating transaction.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")

    # With class
    txn1 = await Mayim.transaction(FoobarExecutor)
    await txn1.begin()
    await txn1.commit()

    # With instance
    executor_instance = Mayim.get(FoobarExecutor)
    txn2 = await Mayim.transaction(executor_instance)
    await txn2.begin()
    await txn2.commit()

    # Mixed
    class AnotherExecutor(PostgresExecutor):
        pass

    Mayim(executors=[AnotherExecutor], dsn="postgres://localhost/test")
    another_instance = Mayim.get(AnotherExecutor)

    txn3 = await Mayim.transaction(FoobarExecutor, another_instance)
    await txn3.begin()
    await txn3.commit()


@pytest.mark.xfail(reason="Optional advanced features not fully implemented")
async def test_explicit_api_transaction_properties():
    """
    SPECIFICATION: Transaction object properties and methods.

    Define the full API surface of the transaction object.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)

    # Properties
    assert hasattr(txn, "is_active")
    assert hasattr(txn, "is_committed")
    assert hasattr(txn, "is_rolled_back")
    assert hasattr(txn, "executors")  # List of executors in transaction

    # Methods
    assert hasattr(txn, "begin")
    assert hasattr(txn, "commit")
    assert hasattr(txn, "rollback")

    # Optional advanced features
    if hasattr(txn, "savepoint"):
        # Savepoint support
        await txn.begin()
        sp = await txn.savepoint("sp1")
        # ... operations ...
        await sp.rollback()  # or sp.release()

    if hasattr(txn, "get_metrics"):
        # Metrics support
        await txn.begin()
        await txn.commit()
        metrics = txn.get_metrics()
        assert "duration_ms" in metrics
        assert "operation_count" in metrics


async def test_explicit_api_all_executors_default():
    """
    SPECIFICATION: Default to all executors if none specified.

    If no executors are passed, should include all registered SQL executors.
    """

    class Exec1(PostgresExecutor):
        pass

    class Exec2(PostgresExecutor):
        pass

    Mayim(executors=[Exec1, Exec2], dsn="postgres://localhost/test")

    # No executors specified = all SQL executors
    txn = await Mayim.transaction()
    await txn.begin()

    # Should include both executors
    assert len(txn.executors) == 2
    assert Exec1 in [type(e) if not isinstance(e, type) else e for e in txn.executors]
    assert Exec2 in [type(e) if not isinstance(e, type) else e for e in txn.executors]

    await txn.commit()


@pytest.mark.xfail(reason="Isolation level feature not implemented")
async def test_explicit_api_isolation_level():
    """
    SPECIFICATION: Support transaction isolation levels.

    Should allow setting isolation level for the transaction.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    # Optional feature: isolation level
    txn = await Mayim.transaction(
        FoobarExecutor,
        isolation_level="SERIALIZABLE",  # or "READ_COMMITTED", "REPEATABLE_READ", etc.
    )
    await txn.begin()

    # Transaction should use specified isolation level
    if hasattr(txn, "isolation_level"):
        assert txn.isolation_level == "SERIALIZABLE"

    await txn.commit()


@pytest.mark.xfail(reason="Read-only transactions not implemented")
async def test_explicit_api_readonly_transactions():
    """
    SPECIFICATION: Support read-only transactions.

    Should allow marking transaction as read-only for optimization.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    # Optional feature: read-only transactions
    txn = await Mayim.transaction(FoobarExecutor, readonly=True)
    await txn.begin()

    executor = Mayim.get(FoobarExecutor)

    # Read operations should work
    # (In real implementation, write operations would fail)

    await txn.commit()


# ============================================================================
# INTERACTION WITH EXISTING FEATURES
# ============================================================================


async def test_explicit_api_with_nested_executor_transactions():
    """
    SPECIFICATION: Interaction with executor-level transactions.

    When in a global transaction, executor.transaction() should be a no-op.
    """
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)
    await txn.begin()

    executor = Mayim.get(FoobarExecutor)

    # This should be a no-op since we're already in a transaction
    async with executor.transaction():
        await executor.insert_test(value="nested")
        # Should use the global transaction's connection

    await txn.commit()


async def test_explicit_api_connection_sharing_verification(postgres_connection):
    """
    SPECIFICATION: Verify connection sharing in explicit API.

    All executors should share the same database connection.
    """
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    class Exec1(PostgresExecutor):
        @query("SELECT 1")
        async def test_query1(self) -> int:
            ...
            
        @classmethod
        def _load(cls, strict: bool) -> None:
            if not hasattr(cls, "_loaded") or not cls._loaded:
                cls._queries = {}
                cls._hydrators = {}
                cls._loaded = True

    class Exec2(PostgresExecutor):
        @query("SELECT 2")
        async def test_query2(self) -> int:
            ...
            
        @classmethod
        def _load(cls, strict: bool) -> None:
            if not hasattr(cls, "_loaded") or not cls._loaded:
                cls._queries = {}
                cls._hydrators = {}
                cls._loaded = True

    Mayim(executors=[Exec1, Exec2], dsn="postgres://localhost/test")

    connections_used = set()

    # Create a proper async context manager for connection tracking
    @asynccontextmanager
    async def track_connection(*args, **kwargs):
        # Create a mock connection and track it
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        conn_id = id(conn)
        connections_used.add(conn_id)
        yield conn

    exec1 = Mayim.get(Exec1)
    exec2 = Mayim.get(Exec2)
    
    # Replace the pool connection method for both executors
    exec1.pool.connection = track_connection
    exec2.pool.connection = track_connection

    txn = await Mayim.transaction(Exec1, Exec2)
    await txn.begin()

    # Execute queries on both executors to trigger connection usage
    postgres_connection.result = {"?column?": 1}
    await exec1.test_query1()
    
    postgres_connection.result = {"?column?": 2}
    await exec2.test_query2()

    await txn.commit()

    # Should have used only one connection
    assert len(connections_used) == 1, f"Should share single connection, but {len(connections_used)} were used"
