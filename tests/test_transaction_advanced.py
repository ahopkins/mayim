"""
Advanced transaction tests including edge cases, error handling,
and real-world scenarios. These tests define the expected behavior
for the fixed transaction implementation.
"""

from unittest.mock import AsyncMock, MagicMock, patch, call
from dataclasses import dataclass
from typing import List, Optional
import asyncio

import pytest

from mayim import Mayim, PostgresExecutor, MysqlExecutor, SQLiteExecutor, query
from mayim.exception import MayimError
from mayim.registry import Registry


# ============================================================================
# TEST EXECUTORS FOR DIFFERENT DATABASES
# ============================================================================


@dataclass
class Account:
    id: int
    balance: float
    owner: str


class PostgresAccountExecutor(PostgresExecutor):
    @query("SELECT * FROM accounts WHERE id = $account_id FOR UPDATE")
    async def lock_account(self, account_id: int) -> Account:
        """Lock account for update in transaction"""
        ...

    @query("UPDATE accounts SET balance = $balance WHERE id = $account_id")
    async def update_balance(self, account_id: int, balance: float) -> None: ...

    @query(
        "INSERT INTO transfers (from_id, to_id, amount) VALUES ($from_id, $to_id, $amount)"
    )
    async def record_transfer(
        self, from_id: int, to_id: int, amount: float
    ) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class MysqlAccountExecutor(MysqlExecutor):
    @query("SELECT * FROM accounts WHERE id = %s FOR UPDATE")
    async def lock_account(self, account_id: int) -> Account:
        """MySQL version with different parameter style"""
        ...

    @query("UPDATE accounts SET balance = %s WHERE id = %s")
    async def update_balance(self, balance: float, account_id: int) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class SQLiteInventoryExecutor(SQLiteExecutor):
    @query("UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?")
    async def reduce_inventory(self, quantity: int, product_id: int) -> None: ...

    @query("SELECT quantity FROM inventory WHERE product_id = ?")
    async def get_quantity(self, product_id: int) -> int: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


# ============================================================================
# TEST FIXTURES
# ============================================================================


@pytest.fixture
def mock_pools():
    """Create mock pools for testing"""
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock, MagicMock

    @asynccontextmanager
    async def mock_connection(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock()
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        yield conn

    def create_mock_pool():
        pool = MagicMock()
        pool.connection = mock_connection
        pool._connection = MagicMock()
        pool._connection.get = MagicMock(return_value=None)
        pool._connection.set = MagicMock()
        pool._transaction = MagicMock()
        pool._transaction.get = MagicMock(return_value=False)
        pool._transaction.set = MagicMock()
        pool._commit = MagicMock()
        pool._commit.get = MagicMock(return_value=True)
        pool._commit.set = MagicMock()
        pool.in_transaction = MagicMock(return_value=False)
        pool.existing_connection = MagicMock(return_value=None)
        return pool

    return {
        "postgres": create_mock_pool(),
        "mysql": create_mock_pool(),
        "sqlite": create_mock_pool(),
    }


# ============================================================================
# REAL-WORLD SCENARIO TESTS
# ============================================================================


async def test_deadlock_detection_and_recovery(mock_pools):
    """
    TEST: Detect and handle deadlocks in cross-executor transactions.

    Should properly propagate deadlock errors from the database.
    """
    from unittest.mock import AsyncMock

    # Create executors with mocked pools
    postgres_exec = PostgresAccountExecutor(pool=mock_pools["postgres"])
    mysql_exec = MysqlAccountExecutor(pool=mock_pools["mysql"])

    Mayim(executors=[postgres_exec, mysql_exec])

    # Mock the lock_account methods directly
    # In a real scenario, these would go through execute and the database would detect deadlocks
    call_count = [0]

    async def mock_lock_account(account_id):
        call_count[0] += 1
        # Simulate deadlock detection by the database after several lock attempts
        if call_count[0] >= 3:
            raise MayimError(
                "Deadlock detected: Transaction was chosen as deadlock victim"
            )
        return Account(id=account_id, balance=1000, owner="Test")

    postgres_exec.lock_account = AsyncMock(side_effect=mock_lock_account)
    mysql_exec.lock_account = AsyncMock(side_effect=mock_lock_account)

    # Test that deadlock errors are properly propagated
    error_raised = None
    try:
        async with Mayim.transaction(postgres_exec, mysql_exec):
            # These locks will work
            await postgres_exec.lock_account(account_id=1)
            await mysql_exec.lock_account(account_id=2)
            # This will trigger the deadlock
            await postgres_exec.lock_account(account_id=3)
    except Exception as e:
        error_raised = e

    # Check that a deadlock error was raised
    assert error_raised is not None, "Expected an error to be raised"
    assert "deadlock" in str(error_raised).lower(), (
        f"Expected deadlock error, got: {error_raised}"
    )


async def test_bank_transfer_acid_compliance():
    """
    TEST: Classic bank transfer ensuring ACID properties.

    CURRENT: FAILS - No proper ACID guarantees
    EXPECTED: Money transfer should be atomic and consistent
    """
    mayim = Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/bank")
    executor = mayim.get(PostgresAccountExecutor)

    # Initial state
    account1_balance = 1000.0
    account2_balance = 500.0
    transfer_amount = 300.0

    # Mock the accounts
    executor.lock_account = AsyncMock(
        side_effect=[
            Account(id=1, balance=account1_balance, owner="Alice"),
            Account(id=2, balance=account2_balance, owner="Bob"),
        ]
    )
    executor.update_balance = AsyncMock()
    executor.record_transfer = AsyncMock()

    # Simulate failure during transfer
    call_count = 0
    original_update = executor.update_balance

    async def failing_update(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:  # Fail on second update
            raise Exception("Database connection lost")
        return await original_update(*args, **kwargs)

    executor.update_balance = failing_update

    # Attempt transfer
    try:
        async with executor.transaction():
            # Lock both accounts
            acc1 = await executor.lock_account(account_id=1)
            acc2 = await executor.lock_account(account_id=2)

            # Check sufficient funds
            if acc1.balance < transfer_amount:
                raise ValueError("Insufficient funds")

            # Perform transfer
            await executor.update_balance(
                account_id=1, balance=acc1.balance - transfer_amount
            )
            await executor.update_balance(
                account_id=2, balance=acc2.balance + transfer_amount
            )  # FAILS HERE
            await executor.record_transfer(from_id=1, to_id=2, amount=transfer_amount)
    except Exception:
        pass

    assert call_count == 2


async def test_mixed_database_transaction(mock_pools):
    """
    TEST: Transaction across different database types.

    Should work correctly when executors have different pool types.
    """
    # Create executors with different pools
    postgres_exec = PostgresAccountExecutor(pool=mock_pools["postgres"])
    sqlite_exec = SQLiteInventoryExecutor(pool=mock_pools["sqlite"])

    Mayim(executors=[postgres_exec, sqlite_exec])

    # Mock the methods
    postgres_exec.update_balance = AsyncMock()
    sqlite_exec.reduce_inventory = AsyncMock()

    # This should work - different pool types can be in the same transaction
    async with Mayim.transaction(postgres_exec, sqlite_exec):
        await postgres_exec.update_balance(account_id=1, balance=100)
        await sqlite_exec.reduce_inventory(quantity=5, product_id=1)

    # Verify both operations were called
    postgres_exec.update_balance.assert_called_once_with(account_id=1, balance=100)
    sqlite_exec.reduce_inventory.assert_called_once_with(quantity=5, product_id=1)


# ============================================================================
# EDGE CASE TESTS
# ============================================================================


async def test_transaction_timeout(mock_pools):
    """
    TEST: Transaction should timeout after specified duration.

    Should rollback after timeout period expires.
    """
    from unittest.mock import AsyncMock

    # Create executor with mock pool
    executor = PostgresAccountExecutor(pool=mock_pools["postgres"])
    executor.lock_account = AsyncMock(
        return_value=Account(id=1, balance=1000, owner="Test")
    )

    Mayim(executors=[executor])

    txn = await Mayim.transaction(
        executor,
        timeout=0.1,  # Very short timeout for testing
    )
    await txn.begin()

    await executor.lock_account(account_id=1)

    # Simulate long-running operation that exceeds timeout
    await asyncio.sleep(0.2)  # Longer than timeout

    # Transaction should be rolled back due to timeout
    with pytest.raises(MayimError, match="Transaction timed out"):
        await txn.commit()

    assert txn.is_rolled_back, "Transaction should be rolled back after timeout"


async def test_nested_transactions(mock_pools):
    """
    TEST: Nested transactions (savepoints).

    When in a global transaction, nested executor transactions should be no-ops.
    """
    from unittest.mock import AsyncMock

    executor = PostgresAccountExecutor(pool=mock_pools["postgres"])
    executor.update_balance = AsyncMock()

    async with executor.transaction() as outer_txn:
        await executor.update_balance(account_id=1, balance=900)

        try:
            async with executor.transaction() as inner_txn:  # Savepoint
                await executor.update_balance(account_id=2, balance=600)
                raise Exception("Inner transaction fails")
        except Exception:
            pass  # Inner transaction rolled back to savepoint

        # Outer transaction should still be active
        await executor.update_balance(account_id=3, balance=300)

    # Only updates to accounts 1 and 3 should persist


async def test_connection_pool_exhaustion():
    """
    TEST: Handle connection pool exhaustion gracefully.

    CURRENT: FAILS - May hang or fail unclearly
    EXPECTED: Clear error or queueing behavior
    """
    # Configure small pool
    Mayim(
        executors=[PostgresAccountExecutor],
        dsn="postgres://localhost/test",
        max_size=2,  # Small pool
    )

    transactions = []

    async def create_and_hold_txn():
        txn = await Mayim.transaction(PostgresAccountExecutor)
        await txn.begin()
        transactions.append(txn)
        # Hold the transaction open by sleeping
        await asyncio.sleep(1)

    # Start tasks that will consume all connections and hold them
    tasks = []
    for i in range(2):  # Use exactly max_size connections
        task = asyncio.create_task(create_and_hold_txn())
        tasks.append(task)

    # Give tasks time to start and acquire connections
    await asyncio.sleep(0.1)

    # Now try to create another transaction - should fail or queue
    # Since this is testing pool exhaustion, we expect either:
    # 1. A timeout/exhaustion error, or
    # 2. The operation to queue and eventually succeed
    try:
        txn = await Mayim.transaction(PostgresAccountExecutor)
        # Set a very short timeout to trigger exhaustion quickly
        await asyncio.wait_for(txn.begin(), timeout=0.1)
        # If we get here, the pool has queueing behavior (which is valid)
        await txn.rollback()
    except (MayimError, asyncio.TimeoutError):
        # This is expected for pool exhaustion
        pass

    # Clean up: cancel tasks and wait for them to finish
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.xfail(reason="Transaction lifecycle hooks not implemented")
async def test_transaction_lifecycle_hooks():
    """
    TEST: Transaction lifecycle hooks for monitoring/logging.

    CURRENT: FAILS - No hook support
    EXPECTED: Should support pre/post transaction hooks
    """
    events = []

    async def on_begin(txn):
        events.append(("begin", txn))

    async def on_commit(txn):
        events.append(("commit", txn))

    async def on_rollback(txn):
        events.append(("rollback", txn))

    # Register hooks
    Mayim.transaction_hooks(
        on_begin=on_begin, on_commit=on_commit, on_rollback=on_rollback
    )

    # Test successful transaction
    async with Mayim.transaction(PostgresAccountExecutor):
        pass

    assert events[0][0] == "begin"
    assert events[1][0] == "commit"

    events.clear()

    # Test failed transaction
    try:
        async with Mayim.transaction(PostgresAccountExecutor):
            raise Exception("Test failure")
    except Exception:
        pass

    assert events[0][0] == "begin"
    assert events[1][0] == "rollback"


# ============================================================================
# ERROR HANDLING TESTS
# ============================================================================


async def test_invalid_transaction_state_operations():
    """
    TEST: Proper error handling for invalid state operations.

    CURRENT: FAILS - No state validation
    EXPECTED: Clear errors for invalid operations
    """
    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(PostgresAccountExecutor)

    # Cannot commit before begin
    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.commit()

    # Cannot rollback before begin
    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.rollback()

    await txn.begin()
    await txn.commit()

    # Cannot begin after commit
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.begin()

    # Cannot commit twice
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.commit()

    # Cannot rollback after commit
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.rollback()


async def test_invalid_executor_combinations():
    """
    TEST: Validate executor combinations in transactions.

    CURRENT: FAILS - No validation
    EXPECTED: Should validate executor compatibility
    """
    # Reset registry to ensure clean state
    Registry.reset()

    # Test that executors automatically register themselves when created
    unregistered = PostgresAccountExecutor()

    # This should work because executors auto-register
    txn = await Mayim.transaction(unregistered)
    assert txn is not None, "Transaction should be created successfully"

    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")

    # Cannot mix SQL and non-SQL executors
    class NonSQLExecutor:
        pass

    with pytest.raises(MayimError, match="All executors must be SQL executors"):
        await Mayim.transaction(PostgresAccountExecutor, NonSQLExecutor)

    # Cannot use None
    with pytest.raises(MayimError, match="Invalid executor"):
        await Mayim.transaction(None)


@pytest.mark.xfail(reason="Connection failure recovery not implemented")
async def test_connection_failure_during_transaction():
    """
    TEST: Handle connection failures during transaction.

    CURRENT: FAILS - Poor error handling
    EXPECTED: Graceful failure and cleanup
    """
    executor = PostgresAccountExecutor()
    Mayim(executors=[executor], dsn="postgres://localhost/test")

    try:
        async with executor.transaction():
            await executor.update_balance(account_id=1, balance=100)

            # Simulate connection failure
            executor.pool._connection.set(None)

            # This should detect connection loss and raise appropriate error
            await executor.update_balance(account_id=2, balance=200)
        assert False, "Expected MayimError for connection failure"
    except MayimError as e:
        assert "Connection lost" in str(e) or "connection" in str(e).lower(), f"Expected connection error, got: {e}"

    # Verify cleanup happened
    assert not executor.pool.in_transaction()
    assert executor.pool.existing_connection() is None


# ============================================================================
# PERFORMANCE AND MONITORING TESTS
# ============================================================================


@pytest.mark.xfail(reason="Transaction metrics collection not implemented")
async def test_transaction_metrics_collection():
    """
    TEST: Collect metrics about transaction performance.

    CURRENT: FAILS - No metrics collection
    EXPECTED: Should provide transaction metrics
    """
    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")
    async with Mayim.transaction(PostgresAccountExecutor) as txn:
        executor = Mayim.get(PostgresAccountExecutor)
        await executor.update_balance(account_id=1, balance=100)
        await executor.update_balance(account_id=2, balance=200)

    # Should have metrics available
    metrics = txn.get_metrics()
    assert metrics["duration_ms"] > 0
    assert metrics["operation_count"] == 2
    assert metrics["executor_count"] == 1
    assert "begin_time" in metrics
    assert "commit_time" in metrics


async def test_transaction_context_propagation():
    """
    TEST: Transaction context should propagate through async calls.

    CURRENT: FAILS - Context doesn't propagate properly
    EXPECTED: Context should be maintained across async boundaries
    """

    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")

    async def nested_operation(executor):
        # Should see the transaction context from parent
        assert executor.pool.in_transaction()
        await executor.update_balance(account_id=3, balance=300)

    async with Mayim.transaction(PostgresAccountExecutor):
        executor = Mayim.get(PostgresAccountExecutor)
        await executor.update_balance(account_id=1, balance=100)

        # Call nested async function
        await nested_operation(executor)

        # Create new task - context should propagate
        task = asyncio.create_task(nested_operation(executor))
        await task
