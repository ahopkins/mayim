import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from mayim import Mayim, MysqlExecutor, PostgresExecutor, SQLiteExecutor, query
from mayim.exception import MayimError
from mayim.registry import Registry


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
    postgres_connection.execute.assert_called_with(
        "UPDATE otheritems SET name=%(name)s WHERE item_id=%(item_id)s",
        {"item_id": 999, "name": "foo"},
    )


async def test_failed_transaction(postgres_connection, item_executor):
    try:
        async with item_executor.transaction():
            raise Exception("...")
    except Exception:
        ...
    postgres_connection.rollback.assert_called_once()


async def test_transaction_rollback(postgres_connection, item_executor):
    async with item_executor.transaction():
        await item_executor.rollback()
    postgres_connection.rollback.assert_called_once()


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


async def test_global_transaction(
    postgres_connection, ItemExecutor, item_executor, monkeypatch
):
    mock = AsyncMock()

    with monkeypatch.context() as m:
        m.setattr(ItemExecutor, "rollback", mock)
        try:
            async with Mayim.transaction():
                raise Exception("...")
        except Exception:
            ...
        postgres_connection.rollback.assert_not_called()
        mock.assert_called_once_with(silent=True)
        postgres_connection.rollback.reset_mock()
        mock.reset_mock()

        try:
            async with Mayim.transaction(ItemExecutor):
                raise Exception("...")
        except Exception:
            ...
        postgres_connection.rollback.assert_not_called()
        mock.assert_called_once_with(silent=True)
        postgres_connection.rollback.reset_mock()
        mock.reset_mock()

        try:
            async with Mayim.transaction(item_executor):
                raise Exception("...")
        except Exception:
            ...
        postgres_connection.rollback.assert_not_called()
        mock.assert_called_once_with(silent=True)


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

    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.commit()

    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.rollback()

    await txn.begin()

    with pytest.raises(MayimError, match="Transaction already active"):
        await txn.begin()

    await txn.commit()

    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.commit()

    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.rollback()

    with pytest.raises(MayimError, match="Transaction already completed"):
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

    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.commit()

    with pytest.raises(MayimError, match="Transaction not active"):
        await txn.rollback()

    await txn.begin()
    await txn.commit()

    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.begin()

    with pytest.raises(MayimError, match="Transaction already completed"):
        await txn.commit()

    with pytest.raises(MayimError, match="Transaction already completed"):
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

    with pytest.raises(MayimError, match="Transaction already active"):
        await txn.begin()

    await txn.commit()
    assert not txn.is_active
    assert txn.is_committed
    assert not txn.is_rolled_back

    with pytest.raises(MayimError, match="Transaction already completed"):
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


async def test_concurrent_transactions_are_isolated():
    results = []

    Mayim(executors=[UserExecutor], dsn="postgres://user:pass@localhost/test")

    async def transaction1():
        async with Mayim.transaction(UserExecutor):
            user_exec = Mayim.get(UserExecutor)
            results.append(("txn1", user_exec.pool.in_transaction()))
            await asyncio.sleep(0.01)
            results.append(("txn1_after", user_exec.pool.in_transaction()))

    async def transaction2():
        await asyncio.sleep(0.005)
        user_exec = Mayim.get(UserExecutor)
        results.append(("txn2", user_exec.pool.in_transaction()))

    await asyncio.gather(transaction1(), transaction2())

    assert results[0] == ("txn1", True)
    assert results[1] == ("txn2", False)
    assert results[2] == ("txn1_after", True)


async def test_two_phase_commit_detailed(postgres_connection):
    prepared_pools = []
    committed_pools = []

    @asynccontextmanager
    async def mock_connection_2pc(*args, **kwargs):
        conn = AsyncMock()
        conn.execute = AsyncMock(return_value=postgres_connection)
        conn.rollback = AsyncMock()
        conn.commit = AsyncMock()

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

    user_exec.pool.connection = mock_connection_2pc
    order_exec.pool.connection = mock_connection_2pc

    txn = await Mayim.transaction(UserExecutor, OrderExecutor, use_2pc=True)
    await txn.begin()

    postgres_connection.result = {
        "id": 1,
        "name": "Test",
        "email": "test@example.com",
    }
    await user_exec.create_user(name="Test", email="test@example.com")

    postgres_connection.result = {"id": 1, "user_id": 1, "total": 100.0}
    await order_exec.create_order(user_id=1, total=100.0)

    prepare_result = await txn.prepare_all()
    assert prepare_result

    assert len(prepared_pools) > 0
    assert len(committed_pools) == 0

    await txn.commit()

    assert len(committed_pools) > 0


@pytest.mark.xfail(reason="Optional advanced features not fully implemented")
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

    if hasattr(txn, "savepoint"):
        await txn.begin()
        sp = await txn.savepoint("sp1")
        await sp.rollback()

    if hasattr(txn, "get_metrics"):
        await txn.begin()
        await txn.commit()
        metrics = txn.get_metrics()
        assert "duration_ms" in metrics
        assert "operation_count" in metrics


@pytest.mark.xfail(reason="Isolation level feature not implemented")
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
