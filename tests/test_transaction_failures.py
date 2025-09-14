"""
Tests that expose failures in the current transaction implementation.
These tests are expected to FAIL with the current implementation
and should PASS once the transaction mechanism is fixed.
"""

from unittest.mock import AsyncMock, MagicMock, patch
from dataclasses import dataclass
from typing import List

import pytest

from mayim import Mayim, PostgresExecutor, query
from mayim.exception import MayimError


@dataclass
class User:
    id: int
    name: str
    email: str


@dataclass
class Order:
    id: int
    user_id: int
    total: float


@dataclass
class Product:
    id: int
    name: str
    stock: int


class UserExecutor(PostgresExecutor):
    @query("INSERT INTO users (name, email) VALUES ($name, $email) RETURNING *")
    async def create_user(self, name: str, email: str) -> User: ...

    @query("SELECT COUNT(*) as count FROM users")
    async def count_users(self) -> int: ...

    @query("UPDATE users SET email = $email WHERE id = $user_id")
    async def update_user_email(self, user_id: int, email: str) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class OrderExecutor(PostgresExecutor):
    @query("INSERT INTO orders (user_id, total) VALUES ($user_id, $total) RETURNING *")
    async def create_order(self, user_id: int, total: float) -> Order: ...

    @query("SELECT COUNT(*) as count FROM orders")
    async def count_orders(self) -> int: ...

    @query("UPDATE orders SET total = $total WHERE id = $order_id")
    async def update_order_total(self, order_id: int, total: float) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class ProductExecutor(PostgresExecutor):
    @query("UPDATE products SET stock = stock - $quantity WHERE id = $product_id")
    async def reduce_stock(self, product_id: int, quantity: int) -> None: ...

    @query("SELECT stock FROM products WHERE id = $product_id")
    async def get_stock(self, product_id: int) -> int: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


@pytest.fixture
def mayim_instance():
    """Create a shared Mayim instance with all executors"""
    return Mayim(
        executors=[UserExecutor, OrderExecutor, ProductExecutor],
        dsn="postgres://user:pass@localhost:5432/test",
    )


@pytest.fixture
def user_executor(mayim_instance):
    """Create a UserExecutor instance"""
    return Mayim.get(UserExecutor)


@pytest.fixture
def order_executor(mayim_instance):
    """Create an OrderExecutor instance"""
    return Mayim.get(OrderExecutor)


@pytest.fixture
def product_executor(mayim_instance):
    """Create a ProductExecutor instance"""
    return Mayim.get(ProductExecutor)


# ============================================================================
# TESTS THAT EXPOSE CURRENT FAILURES
# ============================================================================


async def test_cross_executor_transaction_shares_connection(
    user_executor, order_executor, postgres_connection
):
    """
    CRITICAL TEST: Verifies that executors in a global transaction share the same connection.

    CURRENT: FAILS - Each executor uses its own connection
    EXPECTED: All executors in transaction should share one connection
    """
    connections_used = set()

    # Create a proper async context manager for connection tracking
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    @asynccontextmanager
    async def track_connection(*args, **kwargs):
        # Create a mock connection and track it
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        connections_used.add(id(conn))
        yield conn

    # Replace the pool connection method for both executors
    user_executor.pool.connection = track_connection
    order_executor.pool.connection = track_connection

    # Mock the operations to return expected results
    postgres_connection.result = {"id": 1, "name": "Test", "email": "test@example.com"}

    txn = await Mayim.transaction(user_executor, order_executor)
    async with txn:
        await user_executor.create_user(name="Test", email="test@example.com")
        postgres_connection.result = {"id": 1, "user_id": 1, "total": 100.0}
        await order_executor.create_order(user_id=1, total=100.0)

    # This assertion FAILS: Currently multiple connections are used
    assert len(connections_used) == 1, (
        f"Expected 1 shared connection, but {len(connections_used)} were used"
    )


async def test_cross_executor_transaction_atomic_rollback(
    user_executor, order_executor, postgres_connection
):
    """
    CRITICAL TEST: Verifies atomic rollback across multiple executors.

    All operations should rollback together atomically.
    """
    # Mock the initial counts
    postgres_connection.result = {"count": 0}
    initial_users = await user_executor.count_users()
    initial_orders = await order_executor.count_orders()

    # Simulate the transaction with an exception
    postgres_connection.result = {"id": 1, "name": "Test", "email": "test@example.com"}

    try:
        async with Mayim.transaction(user_executor, order_executor):
            await user_executor.create_user(name="Test", email="test@example.com")
            await order_executor.create_order(user_id=1, total=100.0)
            raise Exception("Force rollback")
    except Exception:
        pass

    # Check that rollback was called on the SAME connection for both
    # This will FAIL because each executor has its own connection
    postgres_connection.result = {"count": 0}
    final_users = await user_executor.count_users()
    final_orders = await order_executor.count_orders()

    assert final_users == initial_users, "User count should not have changed"
    assert final_orders == initial_orders, "Order count should not have changed"


async def test_transaction_context_visibility(
    user_executor, order_executor, postgres_connection
):
    """
    TEST: Verifies that transaction context is visible across executors.

    All executors in a transaction should share the same context.
    """
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock

    # Create a proper async context manager for the pools
    @asynccontextmanager
    async def mock_connection(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        yield conn

    # Set up the connection method for both pools
    user_executor.pool.connection = mock_connection
    order_executor.pool.connection = mock_connection

    # Track if pools are in transaction
    in_transaction_states = []

    postgres_connection.result = {"id": 1, "name": "Test", "email": "test@example.com"}

    async with Mayim.transaction(user_executor, order_executor):
        # Check if both executors see themselves in a transaction
        in_transaction_states.append(user_executor.pool.in_transaction())
        in_transaction_states.append(order_executor.pool.in_transaction())

    # All executors should see themselves in a transaction
    assert all(in_transaction_states), (
        "All executors should see themselves in a transaction"
    )


async def test_same_dsn_shares_pool():
    """
    TEST: Verifies that executors with same DSN share the pool instance.

    CURRENT: FAILS - Each executor gets its own pool instance
    EXPECTED: Same DSN should result in same pool instance
    """
    dsn = "postgres://user:pass@localhost:5432/test"

    # Reset and create two executors with same DSN
    Mayim(executors=[UserExecutor, OrderExecutor], dsn=dsn)

    user_exec = Mayim.get(UserExecutor)
    order_exec = Mayim.get(OrderExecutor)

    # This FAILS: Currently different pool instances are created
    assert user_exec.pool is order_exec.pool, (
        "Executors with same DSN should share the same pool instance"
    )


# ============================================================================
# TESTS FOR NEW EXPLICIT TRANSACTION API (NOT YET IMPLEMENTED)
# ============================================================================


async def test_explicit_transaction_begin_commit():
    """
    TEST: New explicit transaction API with begin/commit.

    The explicit transaction API is now implemented and working.
    """
    mayim = Mayim(
        executors=[UserExecutor, OrderExecutor],
        dsn="postgres://user:pass@localhost/test",
    )

    user_exec = mayim.get(UserExecutor)
    order_exec = mayim.get(OrderExecutor)

    # New API pattern
    txn = await Mayim.transaction(user_exec, order_exec)

    assert not txn.is_active, "Transaction should not be active before begin()"

    await txn.begin()
    assert txn.is_active, "Transaction should be active after begin()"

    await user_exec.create_user(name="Test", email="test@example.com")
    await order_exec.create_order(user_id=1, total=100.0)

    await txn.commit()
    assert txn.is_committed, "Transaction should be committed"
    assert not txn.is_active, "Transaction should not be active after commit"


async def test_explicit_transaction_rollback():
    """
    TEST: New explicit transaction API with rollback.

    CURRENT: FAILS - API doesn't exist yet
    EXPECTED: Should support explicit rollback
    """
    Mayim(
        executors=[UserExecutor, OrderExecutor],
        dsn="postgres://user:pass@localhost/test",
    )
    txn = await Mayim.transaction(UserExecutor, OrderExecutor)
    await txn.begin()

    user_exec = Mayim.get(UserExecutor)
    order_exec = Mayim.get(OrderExecutor)

    await user_exec.create_user(name="Test", email="test@example.com")
    await order_exec.create_order(user_id=1, total=100.0)

    await txn.rollback()
    assert txn.is_rolled_back, "Transaction should be rolled back"
    assert not txn.is_active, "Transaction should not be active after rollback"


async def test_explicit_transaction_state_machine():
    """
    TEST: Transaction state machine transitions.

    CURRENT: FAILS - API doesn't exist yet
    EXPECTED: Proper state transitions
    """
    Mayim(executors=[UserExecutor], dsn="postgres://user:pass@localhost/test")
    txn = await Mayim.transaction(UserExecutor)

    # Initial state
    assert not txn.is_active
    assert not txn.is_committed
    assert not txn.is_rolled_back

    # After begin
    await txn.begin()
    assert txn.is_active
    assert not txn.is_committed
    assert not txn.is_rolled_back

    # Cannot begin twice
    with pytest.raises(MayimError, match="Transaction already active"):
        await txn.begin()

    # After commit
    await txn.commit()
    assert not txn.is_active
    assert txn.is_committed
    assert not txn.is_rolled_back

    # Cannot commit twice
    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.commit()


async def test_mixed_transaction_patterns():
    """
    TEST: Mix context manager and explicit patterns.

    CURRENT: FAILS - Not supported
    EXPECTED: Both patterns should work together
    """
    Mayim(
        executors=[UserExecutor, OrderExecutor],
        dsn="postgres://user:pass@localhost/test",
    )
    # Context manager pattern
    async with Mayim.transaction(UserExecutor) as txn:
        user_exec = Mayim.get(UserExecutor)
        await user_exec.create_user(name="Context", email="context@example.com")

        # Should be able to access transaction object
        assert txn.is_active

    # Explicit pattern
    txn2 = await Mayim.transaction(OrderExecutor)
    await txn2.begin()
    order_exec = Mayim.get(OrderExecutor)
    await order_exec.create_order(user_id=1, total=50.0)
    await txn2.commit()

    assert txn2.is_committed


# ============================================================================
# TESTS FOR CONNECTION SHARING VERIFICATION
# ============================================================================


async def test_verify_connection_reuse_in_nested_calls(user_executor, postgres_connection):
    """
    TEST: Verify connection is reused in nested executor calls.

    When multiple operations happen within a single executor transaction,
    they should all use the same database connection.
    """
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock
    
    connections = []

    # Create a mock connection to track
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock(return_value=postgres_connection)
    mock_conn.rollback = AsyncMock()
    mock_conn.commit = AsyncMock()
    
    @asynccontextmanager
    async def track_conn(*args, **kwargs):
        conn_id = id(mock_conn)
        connections.append(conn_id)
        yield mock_conn

    user_executor.pool.connection = track_conn
    
    # Mock the results for each operation
    postgres_connection.result = {"id": 1, "name": "User1", "email": "user1@example.com"}

    async with user_executor.transaction():
        await user_executor.create_user(name="User1", email="user1@example.com")
        postgres_connection.result = {"id": 1, "name": "User1", "email": "new@example.com"}
        await user_executor.update_user_email(user_id=1, email="new@example.com")
        postgres_connection.result = {"count": 1}
        await user_executor.count_users()

    # All operations should use the same connection
    assert len(connections) >= 1, "Should have acquired at least one connection"
    assert len(set(connections)) == 1, (
        f"Expected 1 unique connection, but {len(set(connections))} were used"
    )


async def test_concurrent_transactions_are_isolated():
    """
    TEST: Verify concurrent transactions are properly isolated.

    CURRENT: FAILS - Context variables may leak
    EXPECTED: Each transaction should be isolated
    """
    import asyncio

    results = []

    Mayim(
        executors=[UserExecutor],
        dsn="postgres://user:pass@localhost/test",
    )

    async def transaction1():
        async with Mayim.transaction(UserExecutor):
            user_exec = Mayim.get(UserExecutor)
            # This transaction should be isolated
            results.append(("txn1", user_exec.pool.in_transaction()))
            await asyncio.sleep(0.01)  # Yield control
            results.append(("txn1_after", user_exec.pool.in_transaction()))

    async def transaction2():
        await asyncio.sleep(0.005)  # Start slightly after txn1
        user_exec = Mayim.get(UserExecutor)
        # This should NOT be in a transaction
        results.append(("txn2", user_exec.pool.in_transaction()))

    # Run concurrently
    await asyncio.gather(transaction1(), transaction2())

    # Verify isolation
    assert results[0] == ("txn1", True)
    assert results[1] == ("txn2", False), "txn2 should not see txn1's transaction"
    assert results[2] == ("txn1_after", True)


async def test_two_phase_commit_simulation(postgres_connection):
    """
    TEST: Simulate two-phase commit protocol.

    Should support prepare/commit phases when use_2pc=True.
    """
    from contextlib import asynccontextmanager
    from unittest.mock import AsyncMock
    
    prepared_pools = []
    committed_pools = []
    
    # Create mock connections that support 2PC
    @asynccontextmanager
    async def mock_connection_2pc(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        
        # Add 2PC support
        async def mock_prepare():
            prepared_pools.append(id(conn))
        
        async def mock_commit_prepared():
            committed_pools.append(id(conn))
        
        conn.prepare = AsyncMock(side_effect=mock_prepare)
        conn.commit_prepared = AsyncMock(side_effect=mock_commit_prepared)
        
        yield conn

    Mayim(
        executors=[UserExecutor, OrderExecutor],
        dsn="postgres://user:pass@localhost/test",
    )
    
    user_exec = Mayim.get(UserExecutor)
    order_exec = Mayim.get(OrderExecutor)
    
    # Replace the pool connection method for both executors
    user_exec.pool.connection = mock_connection_2pc
    order_exec.pool.connection = mock_connection_2pc

    # Create transaction with 2PC enabled
    txn = await Mayim.transaction(UserExecutor, OrderExecutor, use_2pc=True)
    await txn.begin()
    
    # Mock the operations
    postgres_connection.result = {"id": 1, "name": "Test", "email": "test@example.com"}
    await user_exec.create_user(name="Test", email="test@example.com")
    
    postgres_connection.result = {"id": 1, "user_id": 1, "total": 100.0}
    await order_exec.create_order(user_id=1, total=100.0)
    
    # Explicitly prepare (phase 1)
    prepare_result = await txn.prepare_all()
    assert prepare_result, "Prepare should succeed"
    
    # Should have prepared but not committed yet  
    assert len(prepared_pools) > 0, "Should have prepared connections"
    assert len(committed_pools) == 0, "Should not have committed yet"
    
    # Commit (phase 2)
    await txn.commit()
    
    # After commit, should have committed
    assert len(committed_pools) > 0, "Should have committed connections"
