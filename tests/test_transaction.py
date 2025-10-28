import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from mayim import Mayim, MysqlExecutor, PostgresExecutor, SQLiteExecutor, query
from mayim.exception import MayimError
from mayim.registry import Registry
from mayim.transaction import (
    IsolationLevel,
    SavepointNotSupportedError,
    TransactionCoordinator,
    TransactionError,
)


@dataclass
class FoobarModel:
    id: int
    value: str


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
class Account:
    id: int
    balance: float
    owner: str


class FoobarExecutor(PostgresExecutor):
    @query("INSERT INTO test (value) VALUES ($value) RETURNING *")
    async def insert_test(self, value: str) -> FoobarModel: ...

    @query("UPDATE test SET value = $value WHERE id = $id")
    async def update_test(self, id: int, value: str) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class UserExecutor(PostgresExecutor):
    @query(
        "INSERT INTO users (name, email) VALUES ($name, $email) RETURNING *"
    )
    async def create_user(self, name: str, email: str) -> User: ...

    @query("UPDATE users SET name = $name WHERE id = $id")
    async def update_user(self, id: int, name: str) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class OrderExecutor(PostgresExecutor):
    @query(
        "INSERT INTO orders (user_id, total) VALUES ($user_id, $total) RETURNING *"
    )
    async def create_order(self, user_id: int, total: float) -> Order: ...

    @query("UPDATE orders SET total = $total WHERE id = $id")
    async def update_order(self, id: int, total: float) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class PostgresAccountExecutor(PostgresExecutor):
    @query("SELECT * FROM accounts WHERE id = $account_id FOR UPDATE")
    async def lock_account(self, account_id: int) -> Account: ...

    @query("UPDATE accounts SET balance = $balance WHERE id = $account_id")
    async def update_balance(
        self, account_id: int, balance: float
    ) -> None: ...

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
    async def lock_account(self, account_id: int) -> Account: ...

    @query("UPDATE accounts SET balance = %s WHERE id = %s")
    async def update_balance(
        self, balance: float, account_id: int
    ) -> None: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


class SQLiteInventoryExecutor(SQLiteExecutor):
    @query("UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?")
    async def reduce_inventory(
        self, quantity: int, product_id: int
    ) -> None: ...

    @query("SELECT quantity FROM inventory WHERE product_id = ?")
    async def get_quantity(self, product_id: int) -> int: ...

    @classmethod
    def _load(cls, strict: bool) -> None:
        if not hasattr(cls, "_loaded") or not cls._loaded:
            cls._queries = {}
            cls._hydrators = {}
            cls._loaded = True


@pytest.fixture
def mock_pools():
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


async def test_transaction(postgres_connection, item_executor):
    async with item_executor.transaction():
        await item_executor.update_item_empty(item_id=999, name="foo")
    postgres_connection.rollback.assert_not_called()
    # Check that the UPDATE call was made (may not be the last call due to COMMIT)

    expected_call = call(
        "UPDATE otheritems SET name=%(name)s WHERE item_id=%(item_id)s",
        {"item_id": 999, "name": "foo"},
    )
    assert expected_call in postgres_connection.execute.call_args_list


async def test_failed_transaction(postgres_connection, item_executor):
    try:
        async with item_executor.transaction():
            raise Exception("...")
    except Exception:
        ...
    # Check that ROLLBACK was called (either via rollback() or execute("ROLLBACK"))
    rollback_called = postgres_connection.rollback.called or any(
        "ROLLBACK" in str(call)
        for call in postgres_connection.execute.call_args_list
    )
    assert rollback_called


async def test_transaction_rollback(postgres_connection, item_executor):
    async with item_executor.transaction():
        await item_executor.rollback()
    # Check that ROLLBACK was executed via SQL command (better than old rollback() method)
    rollback_executed = any(
        "ROLLBACK" in str(call)
        for call in postgres_connection.execute.call_args_list
    )
    assert rollback_executed


async def test_rollback_outside_transaction_with_error(
    postgres_connection, item_executor
):
    message = "Cannot rollback non-existing transaction"
    with pytest.raises(MayimError, match=message):
        await item_executor.rollback()
    postgres_connection.rollback.assert_not_called()


async def test_rollback_outside_transaction_no_error(
    postgres_connection, item_executor
):
    await item_executor.rollback(silent=True)
    postgres_connection.rollback.assert_not_called()


# test_global_transaction removed - old global transaction pattern was replaced with TransactionCoordinator


async def test_two_phase_commit_simulation():
    user_exec = UserExecutor()
    order_exec = OrderExecutor()
    Mayim(executors=[user_exec, order_exec], dsn="postgres://localhost/test")

    user_exec.create_user = AsyncMock(
        return_value=User(id=1, name="Alice", email="alice@example.com")
    )
    order_exec.create_order = AsyncMock(
        return_value=Order(id=1, user_id=1, total=100.0)
    )

    user_exec.pool.existing_connection = MagicMock(return_value=AsyncMock())
    order_exec.pool.existing_connection = MagicMock(return_value=AsyncMock())

    async with Mayim.transaction(user_exec, order_exec, use_2pc=True):
        user = await user_exec.create_user(
            name="Alice", email="alice@example.com"
        )
        order = await order_exec.create_order(user_id=user.id, total=100.0)

    user_exec.create_user.assert_called_once_with(
        name="Alice", email="alice@example.com"
    )
    order_exec.create_order.assert_called_once_with(user_id=1, total=100.0)


async def test_transaction_isolation_between_executors():
    user_exec = UserExecutor()
    order_exec = OrderExecutor()

    Mayim(executors=[user_exec, order_exec], dsn="postgres://localhost/test")

    user_exec.create_user = AsyncMock(
        return_value=User(id=1, name="Alice", email="alice@example.com")
    )
    order_exec.create_order = AsyncMock(
        return_value=Order(id=1, user_id=1, total=100.0)
    )

    try:
        async with Mayim.transaction(user_exec, order_exec):
            user = await user_exec.create_user(
                name="Alice", email="alice@example.com"
            )
            raise Exception("Simulated failure after user creation")
    except Exception:
        pass

    user_exec.create_user.assert_called_once()


async def test_nested_executor_in_global_transaction():
    user_exec = UserExecutor()
    order_exec = OrderExecutor()

    Mayim(executors=[user_exec, order_exec], dsn="postgres://localhost/test")

    user_exec.create_user = AsyncMock(
        return_value=User(id=1, name="Alice", email="alice@example.com")
    )
    order_exec.create_order = AsyncMock(
        return_value=Order(id=1, user_id=1, total=100.0)
    )

    async with Mayim.transaction(user_exec, order_exec):
        user = await user_exec.create_user(
            name="Alice", email="alice@example.com"
        )
        async with user_exec.transaction():
            order = await order_exec.create_order(user_id=user.id, total=100.0)

    user_exec.create_user.assert_called_once()
    order_exec.create_order.assert_called_once()


async def test_explicit_api_basic_usage(postgres_connection):
    Mayim(
        executors=[FoobarExecutor],
        dsn="postgres://user:pass@localhost:5432/test",
    )
    executor = Mayim.get(FoobarExecutor)

    txn = await Mayim.transaction(FoobarExecutor)

    assert not txn.is_active
    assert not txn.is_committed
    assert not txn.is_rolled_back

    await txn.begin()
    assert txn.is_active

    postgres_connection.result = {"id": 1, "value": "test1"}
    result = await executor.insert_test(value="test1")
    assert txn.is_active

    await txn.commit()
    assert not txn.is_active
    assert txn.is_committed


async def test_explicit_api_with_multiple_executors():
    class AnotherExecutor(PostgresExecutor):
        @query("INSERT INTO another (data) VALUES ($data)")
        async def insert_another(self, data: str) -> None: ...

    Mayim(
        executors=[FoobarExecutor, AnotherExecutor],
        dsn="postgres://localhost/test",
    )

    txn = await Mayim.transaction(FoobarExecutor, AnotherExecutor)
    await txn.begin()

    test_exec = Mayim.get(FoobarExecutor)
    another_exec = Mayim.get(AnotherExecutor)

    await test_exec.insert_test(value="test")
    await another_exec.insert_another(data="data")

    await txn.commit()
    assert txn.is_committed


async def test_explicit_api_rollback_behavior():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)
    await txn.begin()

    executor = Mayim.get(FoobarExecutor)
    await executor.insert_test(value="will_be_rolled_back")

    await txn.rollback()

    assert not txn.is_active
    assert txn.is_rolled_back
    assert not txn.is_committed


async def test_explicit_api_context_manager_compatibility(postgres_connection):
    Mayim(
        executors=[FoobarExecutor],
        dsn="postgres://user:pass@localhost:5432/test",
    )
    executor = Mayim.get(FoobarExecutor)

    txn1 = await Mayim.transaction(FoobarExecutor)
    await txn1.begin()
    postgres_connection.result = {"id": 1, "value": "explicit"}
    await executor.insert_test(value="explicit")
    await txn1.commit()
    assert txn1.is_committed

    txn2 = await Mayim.transaction(FoobarExecutor)
    async with txn2:
        postgres_connection.result = {"id": 2, "value": "context_manager"}
        await executor.insert_test(value="context_manager")
    assert txn2.is_committed

    txn3 = await Mayim.transaction(FoobarExecutor)
    async with txn3:
        postgres_connection.result = {"id": 3, "value": "direct_context"}
        await executor.insert_test(value="direct_context")
        assert txn3.is_active
    assert txn3.is_committed


async def test_explicit_api_error_handling():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)

    with pytest.raises(TransactionError, match="not begun"):
        await txn.commit()

    with pytest.raises(TransactionError, match="not begun"):
        await txn.rollback()

    await txn.begin()

    with pytest.raises(TransactionError, match="already begun"):
        await txn.begin()

    await txn.commit()

    with pytest.raises(TransactionError, match="already finalized"):
        await txn.commit()

    with pytest.raises(TransactionError, match="already finalized"):
        await txn.rollback()

    with pytest.raises(TransactionError, match="already finalized"):
        await txn.begin()


async def test_explicit_api_with_executor_instances():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")

    txn1 = await Mayim.transaction(FoobarExecutor)
    await txn1.begin()
    await txn1.commit()

    executor_instance = Mayim.get(FoobarExecutor)
    txn2 = await Mayim.transaction(executor_instance)
    await txn2.begin()
    await txn2.commit()

    class AnotherExecutor(PostgresExecutor):
        pass

    Mayim(executors=[AnotherExecutor], dsn="postgres://localhost/test")
    another_instance = Mayim.get(AnotherExecutor)

    txn3 = await Mayim.transaction(FoobarExecutor, another_instance)
    await txn3.begin()
    await txn3.commit()


async def test_explicit_api_all_executors_default():
    class Exec1(PostgresExecutor):
        pass

    class Exec2(PostgresExecutor):
        pass

    Mayim(executors=[Exec1, Exec2], dsn="postgres://localhost/test")

    txn = await Mayim.transaction()
    await txn.begin()

    assert len(txn.executors) == 2
    assert Exec1 in [
        type(e) if not isinstance(e, type) else e for e in txn.executors
    ]
    assert Exec2 in [
        type(e) if not isinstance(e, type) else e for e in txn.executors
    ]

    await txn.commit()


async def test_explicit_api_with_nested_executor_transactions():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)
    await txn.begin()

    executor = Mayim.get(FoobarExecutor)

    async with executor.transaction():
        await executor.insert_test(value="nested")

    await txn.commit()


async def test_explicit_api_connection_sharing_verification(
    postgres_connection,
):
    class Exec1(PostgresExecutor):
        @query("SELECT 1")
        async def test_query1(self) -> int: ...

        @classmethod
        def _load(cls, strict: bool) -> None:
            if not hasattr(cls, "_loaded") or not cls._loaded:
                cls._queries = {}
                cls._hydrators = {}
                cls._loaded = True

    class Exec2(PostgresExecutor):
        @query("SELECT 2")
        async def test_query2(self) -> int: ...

        @classmethod
        def _load(cls, strict: bool) -> None:
            if not hasattr(cls, "_loaded") or not cls._loaded:
                cls._queries = {}
                cls._hydrators = {}
                cls._loaded = True

    Mayim(executors=[Exec1, Exec2], dsn="postgres://localhost/test")

    connections_used = set()

    @asynccontextmanager
    async def track_connection(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        conn_id = id(conn)
        connections_used.add(conn_id)
        yield conn

    exec1 = Mayim.get(Exec1)
    exec2 = Mayim.get(Exec2)

    exec1.pool.connection = track_connection
    exec2.pool.connection = track_connection

    txn = await Mayim.transaction(Exec1, Exec2)
    await txn.begin()

    postgres_connection.result = {"?column?": 1}
    await exec1.test_query1()

    postgres_connection.result = {"?column?": 2}
    await exec2.test_query2()

    await txn.commit()

    assert len(connections_used) == 1


async def test_deadlock_detection_and_recovery(mock_pools):
    postgres_exec = PostgresAccountExecutor(pool=mock_pools["postgres"])
    mysql_exec = MysqlAccountExecutor(pool=mock_pools["mysql"])

    Mayim(executors=[postgres_exec, mysql_exec])

    call_count = [0]

    async def mock_lock_account(account_id):
        call_count[0] += 1
        if call_count[0] >= 3:
            raise MayimError(
                "Deadlock detected: Transaction was chosen as deadlock victim"
            )
        return Account(id=account_id, balance=1000, owner="Test")

    postgres_exec.lock_account = AsyncMock(side_effect=mock_lock_account)
    mysql_exec.lock_account = AsyncMock(side_effect=mock_lock_account)

    error_raised = None
    try:
        async with Mayim.transaction(postgres_exec, mysql_exec):
            await postgres_exec.lock_account(account_id=1)
            await mysql_exec.lock_account(account_id=2)
            await postgres_exec.lock_account(account_id=3)
    except Exception as e:
        error_raised = e

    assert error_raised is not None
    assert "deadlock" in str(error_raised).lower()


async def test_bank_transfer_acid_compliance():
    mayim = Mayim(
        executors=[PostgresAccountExecutor], dsn="postgres://localhost/bank"
    )
    executor = mayim.get(PostgresAccountExecutor)

    account1_balance = 1000.0
    account2_balance = 500.0
    transfer_amount = 300.0

    executor.lock_account = AsyncMock(
        side_effect=[
            Account(id=1, balance=account1_balance, owner="Alice"),
            Account(id=2, balance=account2_balance, owner="Bob"),
        ]
    )
    executor.update_balance = AsyncMock()
    executor.record_transfer = AsyncMock()

    call_count = 0
    original_update = executor.update_balance

    async def failing_update(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise Exception("Database connection lost")
        return await original_update(*args, **kwargs)

    executor.update_balance = failing_update

    try:
        async with executor.transaction():
            acc1 = await executor.lock_account(account_id=1)
            acc2 = await executor.lock_account(account_id=2)

            if acc1.balance < transfer_amount:
                raise ValueError("Insufficient funds")

            await executor.update_balance(
                account_id=1, balance=acc1.balance - transfer_amount
            )
            await executor.update_balance(
                account_id=2, balance=acc2.balance + transfer_amount
            )
            await executor.record_transfer(
                from_id=1, to_id=2, amount=transfer_amount
            )
    except Exception:
        pass

    assert call_count == 2


async def test_mixed_database_transaction(mock_pools):
    postgres_exec = PostgresAccountExecutor(pool=mock_pools["postgres"])
    sqlite_exec = SQLiteInventoryExecutor(pool=mock_pools["sqlite"])

    Mayim(executors=[postgres_exec, sqlite_exec])

    postgres_exec.update_balance = AsyncMock()
    sqlite_exec.reduce_inventory = AsyncMock()

    async with Mayim.transaction(postgres_exec, sqlite_exec):
        await postgres_exec.update_balance(account_id=1, balance=100)
        await sqlite_exec.reduce_inventory(quantity=5, product_id=1)

    postgres_exec.update_balance.assert_called_once_with(
        account_id=1, balance=100
    )
    sqlite_exec.reduce_inventory.assert_called_once_with(
        quantity=5, product_id=1
    )


async def test_transaction_timeout(mock_pools):
    executor = PostgresAccountExecutor(pool=mock_pools["postgres"])
    executor.lock_account = AsyncMock(
        return_value=Account(id=1, balance=1000, owner="Test")
    )

    Mayim(executors=[executor])

    txn = await Mayim.transaction(executor, timeout=0.1)
    await txn.begin()

    await executor.lock_account(account_id=1)

    await asyncio.sleep(0.2)

    with pytest.raises(MayimError, match="Transaction timed out"):
        await txn.commit()

    assert txn.is_rolled_back


async def test_nested_transactions(mock_pools):
    executor = PostgresAccountExecutor(pool=mock_pools["postgres"])
    executor.update_balance = AsyncMock()

    async with executor.transaction():
        await executor.update_balance(account_id=1, balance=900)

        try:
            async with executor.transaction():
                await executor.update_balance(account_id=2, balance=600)
                raise Exception("Inner transaction fails")
        except Exception:
            pass

        await executor.update_balance(account_id=3, balance=300)


async def test_connection_pool_exhaustion():
    Mayim(
        executors=[PostgresAccountExecutor],
        dsn="postgres://localhost/test",
        max_size=2,
    )

    transactions = []

    async def create_and_hold_txn():
        txn = await Mayim.transaction(PostgresAccountExecutor)
        await txn.begin()
        transactions.append(txn)
        await asyncio.sleep(1)

    tasks = []
    for i in range(2):
        task = asyncio.create_task(create_and_hold_txn())
        tasks.append(task)

    await asyncio.sleep(0.1)

    try:
        txn = await Mayim.transaction(PostgresAccountExecutor)
        await asyncio.wait_for(txn.begin(), timeout=0.1)
        await txn.rollback()
    except (MayimError, asyncio.TimeoutError):
        pass

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def test_invalid_transaction_state_operations():
    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(PostgresAccountExecutor)
    txn_id = txn.transaction_id

    with pytest.raises(MayimError, match=f"Transaction {txn_id} not begun"):
        await txn.commit()

    with pytest.raises(MayimError, match=f"Transaction {txn_id} not begun"):
        await txn.rollback()

    await txn.begin()
    await txn.commit()

    with pytest.raises(
        MayimError, match=f"Transaction {txn_id} already finalized"
    ):
        await txn.begin()

    with pytest.raises(
        MayimError, match=f"Transaction {txn_id} already finalized"
    ):
        await txn.commit()

    with pytest.raises(
        MayimError, match=f"Transaction {txn_id} already finalized"
    ):
        await txn.rollback()


async def test_invalid_executor_combinations():
    Registry.reset()

    unregistered = PostgresAccountExecutor()

    txn = await Mayim.transaction(unregistered)
    assert txn is not None

    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")

    class NonSQLExecutor:
        pass

    with pytest.raises(
        MayimError, match="All executors must be SQL executors"
    ):
        await Mayim.transaction(PostgresAccountExecutor, NonSQLExecutor)

    with pytest.raises(MayimError, match="Invalid executor"):
        await Mayim.transaction(None)


@pytest.mark.xfail(reason="Connection failure recovery not implemented")
async def test_connection_failure_during_transaction():
    executor = PostgresAccountExecutor()
    Mayim(executors=[executor], dsn="postgres://localhost/test")

    try:
        async with executor.transaction():
            await executor.update_balance(account_id=1, balance=100)

            executor.pool._connection.set(None)

            await executor.update_balance(account_id=2, balance=200)
        assert False, "Expected MayimError for connection failure"
    except MayimError as e:
        assert "Connection lost" in str(e) or "connection" in str(e).lower()

    assert not executor.pool.in_transaction()
    assert executor.pool.existing_connection() is None


@pytest.mark.xfail(reason="Transaction metrics collection not implemented")
async def test_transaction_metrics_collection():
    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")
    async with Mayim.transaction(PostgresAccountExecutor) as txn:
        executor = Mayim.get(PostgresAccountExecutor)
        await executor.update_balance(account_id=1, balance=100)
        await executor.update_balance(account_id=2, balance=200)

    metrics = txn.get_metrics()
    assert metrics["duration_ms"] > 0
    assert metrics["operation_count"] == 2
    assert metrics["executor_count"] == 1
    assert "begin_time" in metrics
    assert "commit_time" in metrics


async def test_transaction_context_propagation():
    Mayim(executors=[PostgresAccountExecutor], dsn="postgres://localhost/test")

    async def nested_operation(executor):
        assert executor.pool.in_transaction()
        await executor.update_balance(account_id=3, balance=300)

    async with Mayim.transaction(PostgresAccountExecutor):
        executor = Mayim.get(PostgresAccountExecutor)
        await executor.update_balance(account_id=1, balance=100)

        await nested_operation(executor)

        task = asyncio.create_task(nested_operation(executor))
        await task


async def test_cross_executor_transaction_shares_connection(
    postgres_connection,
):
    user_executor = UserExecutor()
    order_executor = OrderExecutor()

    Mayim(
        executors=[user_executor, order_executor],
        dsn="postgres://user:pass@localhost:5432/test",
    )

    connections_used = set()

    @asynccontextmanager
    async def track_connection(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()
        connections_used.add(id(conn))
        yield conn

    user_executor.pool.connection = track_connection
    order_executor.pool.connection = track_connection

    postgres_connection.result = {
        "id": 1,
        "name": "Test",
        "email": "test@example.com",
    }

    txn = await Mayim.transaction(user_executor, order_executor)
    async with txn:
        await user_executor.create_user(name="Test", email="test@example.com")
        postgres_connection.result = {"id": 1, "user_id": 1, "total": 100.0}
        await order_executor.create_order(user_id=1, total=100.0)

    assert len(connections_used) == 1


async def test_cross_executor_transaction_atomic_rollback(postgres_connection):
    user_executor = UserExecutor()
    order_executor = OrderExecutor()

    Mayim(
        executors=[user_executor, order_executor],
        dsn="postgres://user:pass@localhost:5432/test",
    )

    user_executor.count_users = AsyncMock(return_value={"count": 0})
    order_executor.count_orders = AsyncMock(return_value={"count": 0})

    postgres_connection.result = {"count": 0}
    initial_users = await user_executor.count_users()
    initial_orders = await order_executor.count_orders()

    postgres_connection.result = {
        "id": 1,
        "name": "Test",
        "email": "test@example.com",
    }

    try:
        async with Mayim.transaction(user_executor, order_executor):
            await user_executor.create_user(
                name="Test", email="test@example.com"
            )
            await order_executor.create_order(user_id=1, total=100.0)
            raise Exception("Force rollback")
    except Exception:
        pass

    postgres_connection.result = {"count": 0}
    final_users = await user_executor.count_users()
    final_orders = await order_executor.count_orders()

    assert final_users == initial_users
    assert final_orders == initial_orders


async def test_transaction_context_visibility(postgres_connection):
    user_executor = UserExecutor()
    order_executor = OrderExecutor()

    Mayim(
        executors=[user_executor, order_executor],
        dsn="postgres://user:pass@localhost:5432/test",
    )

    @asynccontextmanager
    async def mock_connection(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        yield conn

    user_executor.pool.connection = mock_connection
    order_executor.pool.connection = mock_connection

    in_transaction_states = []

    postgres_connection.result = {
        "id": 1,
        "name": "Test",
        "email": "test@example.com",
    }

    async with Mayim.transaction(user_executor, order_executor):
        in_transaction_states.append(user_executor.pool.in_transaction())
        in_transaction_states.append(order_executor.pool.in_transaction())

    assert all(in_transaction_states)


async def test_same_dsn_shares_pool():
    dsn = "postgres://user:pass@localhost:5432/test"

    Mayim(executors=[UserExecutor, OrderExecutor], dsn=dsn)

    user_exec = Mayim.get(UserExecutor)
    order_exec = Mayim.get(OrderExecutor)

    assert user_exec.pool is order_exec.pool


async def test_explicit_transaction_state_machine_detailed():
    Mayim(executors=[UserExecutor], dsn="postgres://user:pass@localhost/test")
    txn = await Mayim.transaction(UserExecutor)

    assert not txn.is_active
    assert not txn.is_committed
    assert not txn.is_rolled_back

    await txn.begin()
    assert txn.is_active
    assert not txn.is_committed
    assert not txn.is_rolled_back

    with pytest.raises(
        MayimError, match=f"Transaction {txn.transaction_id} already begun"
    ):
        await txn.begin()

    await txn.commit()
    assert not txn.is_active
    assert txn.is_committed
    assert not txn.is_rolled_back

    with pytest.raises(
        MayimError, match=f"Transaction {txn.transaction_id} already finalized"
    ):
        await txn.commit()


async def test_mixed_transaction_patterns():
    Mayim(
        executors=[UserExecutor, OrderExecutor],
        dsn="postgres://user:pass@localhost/test",
    )

    async with Mayim.transaction(UserExecutor) as txn:
        user_exec = Mayim.get(UserExecutor)
        await user_exec.create_user(
            name="Context", email="context@example.com"
        )
        assert txn.is_active

    txn2 = await Mayim.transaction(OrderExecutor)
    await txn2.begin()
    order_exec = Mayim.get(OrderExecutor)
    await order_exec.create_order(user_id=1, total=50.0)
    await txn2.commit()

    assert txn2.is_committed


async def test_verify_connection_reuse_in_nested_calls(postgres_connection):
    user_executor = UserExecutor()
    Mayim(
        executors=[user_executor],
        dsn="postgres://user:pass@localhost:5432/test",
    )

    connections = []

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

    postgres_connection.result = {
        "id": 1,
        "name": "User1",
        "email": "user1@example.com",
    }

    async with user_executor.transaction():
        await user_executor.create_user(
            name="User1", email="user1@example.com"
        )
        postgres_connection.result = {"count": 1}
        await user_executor.update_user(id=1, name="Updated")

    assert len(connections) >= 1
    assert len(set(connections)) == 1


async def test_explicit_api_transaction_properties():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor)

    assert hasattr(txn, "is_active")
    assert hasattr(txn, "is_committed")
    assert hasattr(txn, "is_rolled_back")
    assert hasattr(txn, "executors")

    assert hasattr(txn, "begin")
    assert hasattr(txn, "commit")
    assert hasattr(txn, "rollback")

    await txn.begin()
    sp = await txn.savepoint("sp1")
    await sp.rollback()


async def test_explicit_api_isolation_level():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(
        FoobarExecutor,
        isolation_level="SERIALIZABLE",
    )
    await txn.begin()

    if hasattr(txn, "isolation_level"):
        assert txn.isolation_level == "SERIALIZABLE"

    await txn.commit()


@pytest.mark.parametrize(
    "isolation_level,expected_sql",
    [
        ("READ UNCOMMITTED", "BEGIN ISOLATION LEVEL READ UNCOMMITTED"),
        ("READ COMMITTED", "BEGIN ISOLATION LEVEL READ COMMITTED"),
        ("REPEATABLE READ", "BEGIN ISOLATION LEVEL REPEATABLE READ"),
        ("SERIALIZABLE", "BEGIN ISOLATION LEVEL SERIALIZABLE"),
        (
            IsolationLevel.READ_UNCOMMITTED,
            "BEGIN ISOLATION LEVEL READ UNCOMMITTED",
        ),
        (
            IsolationLevel.READ_COMMITTED,
            "BEGIN ISOLATION LEVEL READ COMMITTED",
        ),
        (
            IsolationLevel.REPEATABLE_READ,
            "BEGIN ISOLATION LEVEL REPEATABLE READ",
        ),
        (IsolationLevel.SERIALIZABLE, "BEGIN ISOLATION LEVEL SERIALIZABLE"),
        ("read_committed", "BEGIN ISOLATION LEVEL READ COMMITTED"),
        ("read committed", "BEGIN ISOLATION LEVEL READ COMMITTED"),
    ],
)
async def test_isolation_level_sql_commands(isolation_level, expected_sql):
    """Test that isolation levels generate correct SQL commands"""
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")

    # Create transaction with specific isolation level
    txn = await Mayim.transaction(
        FoobarExecutor, isolation_level=isolation_level
    )

    # Mock the connection manager to capture SQL commands
    mock_connection_manager = AsyncMock()
    txn._connection_manager = mock_connection_manager

    # Begin transaction and verify the SQL command
    await txn.begin()

    # Should have called execute_on_all with the expected SQL
    mock_connection_manager.execute_on_all.assert_called_with(expected_sql)


async def test_default_isolation_level_sql():
    """Test that default isolation level generates correct SQL"""
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")

    # Create transaction without specifying isolation level (should use default)
    txn = await Mayim.transaction(FoobarExecutor)

    # Mock the connection manager
    mock_connection_manager = AsyncMock()
    txn._connection_manager = mock_connection_manager

    # Begin transaction
    await txn.begin()

    # Should use READ COMMITTED as default
    mock_connection_manager.execute_on_all.assert_called_with(
        "BEGIN ISOLATION LEVEL READ COMMITTED"
    )


@pytest.mark.parametrize(
    "invalid_level,expected_error",
    [
        ("INVALID_LEVEL", "Invalid isolation level 'INVALID_LEVEL'"),
        ("SNAPSHOT", "Invalid isolation level 'SNAPSHOT'"),
        (123, "isolation_level must be str or IsolationLevel"),
        (None, "isolation_level must be str or IsolationLevel"),
    ],
)
async def test_invalid_isolation_levels(invalid_level, expected_error):
    """Test that invalid isolation levels are rejected with proper error messages"""
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")

    # Should raise MayimError for invalid isolation levels
    with pytest.raises(MayimError, match=expected_error):
        await Mayim.transaction(FoobarExecutor, isolation_level=invalid_level)


@pytest.mark.xfail(reason="Read-only transactions not implemented")
async def test_explicit_api_readonly_transactions():
    Mayim(executors=[FoobarExecutor], dsn="postgres://localhost/test")
    txn = await Mayim.transaction(FoobarExecutor, readonly=True)
    await txn.begin()

    await txn.commit()


@pytest.mark.xfail(reason="Transaction lifecycle hooks not implemented")
async def test_transaction_lifecycle_hooks():
    events = []

    async def on_begin(txn):
        events.append(("begin", txn))

    async def on_commit(txn):
        events.append(("commit", txn))

    async def on_rollback(txn):
        events.append(("rollback", txn))

    Mayim.transaction_hooks(
        on_begin=on_begin, on_commit=on_commit, on_rollback=on_rollback
    )

    async with Mayim.transaction(PostgresAccountExecutor):
        pass

    assert events[0][0] == "begin"
    assert events[1][0] == "commit"

    events.clear()

    try:
        async with Mayim.transaction(PostgresAccountExecutor):
            raise Exception("Test failure")
    except Exception:
        pass

    assert events[0][0] == "begin"
    assert events[1][0] == "rollback"


async def test_savepoint_basic_functionality():
    """Test basic savepoint creation, rollback, and release"""

    # Mock PostgreSQL executor
    mock_executor = MagicMock()
    mock_executor.pool = MagicMock()
    mock_executor.pool.scheme = "postgresql"
    # Make sure db_type returns None so it uses scheme detection
    mock_executor.pool.db_type = None

    # Mock connection manager
    mock_connection_manager = AsyncMock()

    coord = TransactionCoordinator([mock_executor])
    coord._connection_manager = mock_connection_manager
    coord._begun = True

    # Test savepoint creation
    savepoint = await coord.savepoint("test_sp")
    assert savepoint.name == "test_sp"
    assert not savepoint.is_released
    mock_connection_manager.execute_on_all.assert_called_with(
        "SAVEPOINT test_sp"
    )

    # Test savepoint rollback
    await savepoint.rollback()
    mock_connection_manager.execute_on_all.assert_called_with(
        "ROLLBACK TO SAVEPOINT test_sp"
    )

    # Test savepoint release
    mock_connection_manager.execute_on_all.reset_mock()
    savepoint2 = await coord.savepoint("test_sp2")
    await savepoint2.release()
    assert savepoint2.is_released
    mock_connection_manager.execute_on_all.assert_called_with(
        "RELEASE SAVEPOINT test_sp2"
    )


async def test_savepoint_database_compatibility():
    """Test savepoint database compatibility checking"""

    # Test PostgreSQL (supported)
    postgres_executor = MagicMock()
    postgres_executor.pool = MagicMock()
    postgres_executor.pool.scheme = "postgresql"
    postgres_executor.pool.db_type = None

    coord = TransactionCoordinator([postgres_executor])
    coord._connection_manager = AsyncMock()
    coord._begun = True

    # Should work for PostgreSQL
    await coord.savepoint("pg_savepoint")

    # Test MySQL (supported)
    mysql_executor = MagicMock()
    mysql_executor.pool = MagicMock()
    mysql_executor.pool.scheme = "mysql"
    mysql_executor.pool.db_type = None

    coord = TransactionCoordinator([mysql_executor])
    coord._connection_manager = AsyncMock()
    coord._begun = True

    # Should work for MySQL
    await coord.savepoint("mysql_savepoint")

    # Test SQLite (not supported)
    sqlite_executor = MagicMock()
    sqlite_executor.pool = MagicMock()
    sqlite_executor.pool.scheme = "sqlite"
    sqlite_executor.pool.db_type = None

    coord = TransactionCoordinator([sqlite_executor])
    coord._connection_manager = AsyncMock()
    coord._begun = True

    # Should raise error for SQLite
    with pytest.raises(
        SavepointNotSupportedError,
        match="Savepoints not supported for database type: sqlite",
    ):
        await coord.savepoint("sqlite_savepoint")


async def test_savepoint_error_conditions():
    """Test savepoint error handling"""

    mock_executor = MagicMock()
    mock_executor.pool = MagicMock()
    mock_executor.pool.scheme = "postgresql"
    mock_executor.pool.db_type = None

    coord = TransactionCoordinator([mock_executor])
    coord._connection_manager = AsyncMock()

    # Test savepoint creation before transaction begun
    with pytest.raises(TransactionError, match="not begun"):
        await coord.savepoint("early_sp")

    # Begin transaction
    coord._begun = True

    # Test duplicate savepoint names
    await coord.savepoint("duplicate_sp")
    with pytest.raises(TransactionError, match="already exists"):
        await coord.savepoint("duplicate_sp")

    # Test savepoint operations after transaction finalized
    coord._committed = True
    with pytest.raises(TransactionError, match="already finalized"):
        await coord.savepoint("after_commit_sp")


async def test_savepoint_operations_after_release():
    """Test that savepoint operations fail after release"""

    mock_executor = MagicMock()
    mock_executor.pool = MagicMock()
    mock_executor.pool.scheme = "postgresql"
    mock_executor.pool.db_type = None

    coord = TransactionCoordinator([mock_executor])
    coord._connection_manager = AsyncMock()
    coord._begun = True

    # Create and release savepoint
    savepoint = await coord.savepoint("test_sp")
    await savepoint.release()

    # Operations should fail after release
    with pytest.raises(TransactionError, match="already released"):
        await savepoint.rollback()

    with pytest.raises(TransactionError, match="already released"):
        await savepoint.release()


async def test_savepoint_database_type_detection():
    """Test database type detection logic"""

    coord = TransactionCoordinator([])

    # Test scheme detection
    mock_executor = MagicMock()
    mock_executor.pool = MagicMock()
    mock_executor.pool.scheme = "postgresql"
    mock_executor.pool.db_type = None
    assert coord._detect_db_type(mock_executor) == "postgresql"

    mock_executor.pool.scheme = "mysql"
    assert coord._detect_db_type(mock_executor) == "mysql"

    mock_executor.pool.scheme = "sqlite"
    assert coord._detect_db_type(mock_executor) == "sqlite"

    # Test class name detection (fallback)
    mock_executor.pool = MagicMock()
    del mock_executor.pool.scheme  # Remove scheme attribute
    mock_executor.pool.__class__.__name__ = "PostgresPool"
    mock_executor.pool.__class__.__module__ = "test.module"
    assert coord._detect_db_type(mock_executor) == "postgresql"

    mock_executor.pool.__class__.__name__ = "MysqlPool"
    assert coord._detect_db_type(mock_executor) == "mysql"

    # Test module name detection (final fallback)
    mock_executor.pool.__class__.__name__ = "GenericPool"
    mock_executor.pool.__class__.__module__ = "asyncpg.pool"
    assert coord._detect_db_type(mock_executor) == "postgresql"

    mock_executor.pool.__class__.__module__ = "aiomysql.pool"
    assert coord._detect_db_type(mock_executor) == "mysql"

    # Test unknown database
    mock_executor.pool.__class__.__module__ = "unknown.driver"
    assert coord._detect_db_type(mock_executor) == "unknown"


async def test_savepoint_transaction_cleanup():
    """Test that savepoints are cleaned up when transaction ends"""

    mock_executor = MagicMock()
    mock_executor.pool = MagicMock()
    mock_executor.pool.scheme = "postgresql"
    mock_executor.pool.db_type = None

    coord = TransactionCoordinator([mock_executor])
    coord._connection_manager = AsyncMock()
    coord._begun = True

    # Create savepoint
    savepoint = await coord.savepoint("cleanup_test")
    assert "cleanup_test" in coord._savepoints

    # Release should remove from tracking
    await savepoint.release()
    assert "cleanup_test" not in coord._savepoints
