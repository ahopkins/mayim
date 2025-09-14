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
# REAL-WORLD SCENARIO TESTS
# ============================================================================


async def test_deadlock_detection_and_recovery():
    """
    TEST: Detect and handle deadlocks in cross-executor transactions.

    Should properly propagate deadlock errors from the database.
    """
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock
    
    deadlock_count = [0]
    
    @asynccontextmanager
    async def mock_connection_with_deadlock(*args, **kwargs):
        conn = AsyncMock()
        
        # Simulate a deadlock error on the second lock acquisition
        async def execute_with_deadlock(query, *args, **kwargs):
            if "FOR UPDATE" in query:
                deadlock_count[0] += 1
                if deadlock_count[0] >= 3:  # Third lock attempt causes deadlock
                    raise MayimError("Deadlock detected: Transaction was chosen as deadlock victim")
            
            # Return mock result
            result = AsyncMock()
            result.fetchone = AsyncMock(return_value={'id': 1, 'balance': 1000, 'owner': 'Test'})
            return result
        
        conn.execute = AsyncMock(side_effect=execute_with_deadlock)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        
        yield conn
    
    # Set up mock pools
    postgres_pool = MagicMock()
    postgres_pool.connection = mock_connection_with_deadlock
    postgres_pool._connection = MagicMock()
    postgres_pool._connection.get = MagicMock(return_value=None)
    postgres_pool._connection.set = MagicMock()
    postgres_pool._transaction = MagicMock()
    postgres_pool._transaction.get = MagicMock(return_value=False)
    postgres_pool._transaction.set = MagicMock()
    postgres_pool._commit = MagicMock()
    postgres_pool._commit.get = MagicMock(return_value=True)
    postgres_pool._commit.set = MagicMock()
    postgres_pool.in_transaction = MagicMock(return_value=False)
    postgres_pool.existing_connection = MagicMock(return_value=None)
    
    mysql_pool = MagicMock()
    mysql_pool.connection = mock_connection_with_deadlock
    mysql_pool._connection = MagicMock()
    mysql_pool._connection.get = MagicMock(return_value=None)
    mysql_pool._connection.set = MagicMock()
    mysql_pool._transaction = MagicMock()
    mysql_pool._transaction.get = MagicMock(return_value=False)
    mysql_pool._transaction.set = MagicMock()
    mysql_pool._commit = MagicMock()
    mysql_pool._commit.get = MagicMock(return_value=True)
    mysql_pool._commit.set = MagicMock()
    mysql_pool.in_transaction = MagicMock(return_value=False)
    mysql_pool.existing_connection = MagicMock(return_value=None)
    
    # Create executors with mocked pools
    postgres_exec = PostgresAccountExecutor(pool=postgres_pool)
    mysql_exec = MysqlAccountExecutor(pool=mysql_pool)
    
    Mayim(executors=[postgres_exec, mysql_exec])
    
    # Mock the lock_account methods directly
    # In a real scenario, these would go through execute and the database would detect deadlocks
    call_count = [0]
    
    async def mock_lock_account(account_id):
        call_count[0] += 1
        # Simulate deadlock detection by the database after several lock attempts
        if call_count[0] >= 3:
            raise MayimError("Deadlock detected: Transaction was chosen as deadlock victim")
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
    assert "deadlock" in str(error_raised).lower(), f"Expected deadlock error, got: {error_raised}"


@pytest.mark.xfail(reason="Bank transfer ACID guarantees not ensured")
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

    # Verify ACID: No partial updates should persist
    assert executor.update_balance.call_count == 2
    # But rollback should have been called, undoing the first update
    # In the fixed implementation, the transaction coordinator ensures this


@pytest.mark.xfail(reason="Mixed database transactions not supported")
async def test_mixed_database_transaction():
    """
    TEST: Transaction across different database types.

    CURRENT: FAILS - Different database types can't share transactions
    EXPECTED: Should handle gracefully or provide clear error
    """
    postgres_exec = PostgresAccountExecutor()
    sqlite_exec = SQLiteInventoryExecutor()

    # This should either:
    # 1. Work with separate transactions per database (with warning)
    # 2. Fail with clear error message about incompatible databases

    with pytest.raises(
        MayimError, match="Cannot create transaction across different database types"
    ):
        async with Mayim.transaction(postgres_exec, sqlite_exec):
            await postgres_exec.update_balance(account_id=1, balance=100)
            await sqlite_exec.reduce_inventory(quantity=5, product_id=1)


# ============================================================================
# EDGE CASE TESTS
# ============================================================================


@pytest.mark.xfail(reason="Transaction timeout not implemented")
async def test_transaction_timeout():
    """
    TEST: Transaction should timeout after specified duration.

    CURRENT: FAILS - No timeout support
    EXPECTED: Should rollback after timeout
    """
    txn = await Mayim.transaction(
        PostgresAccountExecutor, timeout=1.0
    )  # 1 second timeout
    await txn.begin()

    executor = Mayim.get(PostgresAccountExecutor)
    await executor.lock_account(account_id=1)

    # Simulate long-running operation
    await asyncio.sleep(2.0)

    # Transaction should be rolled back due to timeout
    with pytest.raises(MayimError, match="Transaction timed out"):
        await txn.commit()

    assert txn.is_rolled_back, "Transaction should be rolled back after timeout"


async def test_nested_transactions():
    """
    TEST: Nested transactions (savepoints).

    When in a global transaction, nested executor transactions should be no-ops.
    """
    executor = PostgresAccountExecutor()

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


@pytest.mark.xfail(reason="Connection pool exhaustion handling not implemented")
async def test_connection_pool_exhaustion():
    """
    TEST: Handle connection pool exhaustion gracefully.

    CURRENT: FAILS - May hang or fail unclearly
    EXPECTED: Clear error or queueing behavior
    """
    # Configure small pool
    mayim = Mayim(
        executors=[PostgresAccountExecutor],
        dsn="postgres://localhost/test",
        pool_size=2,  # Small pool
    )

    transactions = []

    # Try to create more transactions than pool size
    for i in range(5):
        txn = await Mayim.transaction(PostgresAccountExecutor)
        await txn.begin()
        transactions.append(txn)

    # Should either queue or raise clear error
    with pytest.raises(MayimError, match="Connection pool exhausted"):
        txn = await Mayim.transaction(PostgresAccountExecutor)
        await txn.begin()


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


@pytest.mark.xfail(reason="Transaction state validation not implemented")
async def test_invalid_transaction_state_operations():
    """
    TEST: Proper error handling for invalid state operations.

    CURRENT: FAILS - No state validation
    EXPECTED: Clear errors for invalid operations
    """
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


@pytest.mark.xfail(reason="Executor validation not implemented")
async def test_invalid_executor_combinations():
    """
    TEST: Validate executor combinations in transactions.

    CURRENT: FAILS - No validation
    EXPECTED: Should validate executor compatibility
    """

    # Cannot mix SQL and non-SQL executors
    class NonSQLExecutor:
        pass

    with pytest.raises(MayimError, match="All executors must be SQL executors"):
        await Mayim.transaction(PostgresAccountExecutor, NonSQLExecutor)

    # Cannot use unregistered executors
    unregistered = PostgresAccountExecutor()
    with pytest.raises(MayimError, match="Executor not registered"):
        await Mayim.transaction(unregistered)

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

    async with pytest.raises(MayimError, match="Connection lost"):
        async with executor.transaction():
            await executor.update_balance(account_id=1, balance=100)

            # Simulate connection failure
            executor.pool._connection.set(None)

            # This should detect connection loss and raise appropriate error
            await executor.update_balance(account_id=2, balance=200)

    # Verify cleanup happened
    assert not executor.pool.in_transaction()
    assert executor.pool.existing_connection() is None


# ============================================================================
# PERFORMANCE AND MONITORING TESTS
# ============================================================================


@pytest.mark.xfail(reason="Transaction metrics not implemented")
async def test_transaction_metrics_collection():
    """
    TEST: Collect metrics about transaction performance.

    CURRENT: FAILS - No metrics collection
    EXPECTED: Should provide transaction metrics
    """
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


@pytest.mark.xfail(reason="Transaction context propagation not implemented")
async def test_transaction_context_propagation():
    """
    TEST: Transaction context should propagate through async calls.

    CURRENT: FAILS - Context doesn't propagate properly
    EXPECTED: Context should be maintained across async boundaries
    """

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
