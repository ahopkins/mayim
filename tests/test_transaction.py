import pytest

from mayim.exception import MayimError


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


async def test_rollback_outside_transaction(
    postgres_connection, item_executor
):
    message = "Cannot rollback non-existing transaction"
    with pytest.raises(MayimError, match=message):
        await item_executor.rollback()
    postgres_connection.rollback.assert_not_called()
