import re
from typing import List

import pytest

from mayim import Mayim, PostgresExecutor, sql
from mayim.exception import RecordNotFound


async def test_empty_result_single(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @sql("SELECT * FROM otheritems WHERE item_id=$item_id")
        async def select_item(self, item_id: int) -> int:
            ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    message = re.escape(
        "Query <select_item> did not find any record using "
        "() and {'item_id': 999}"
    )
    with pytest.raises(RecordNotFound, match=message):
        await executor.select_item(item_id=999)


async def test_empty_result_multiple(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @sql("SELECT * FROM otheritems")
        async def select_items(self) -> List[int]:
            ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    result = await executor.select_items()
    assert result == []
