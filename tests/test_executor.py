import re
import sys
from typing import List, Optional

import pytest

from mayim import Mayim, PostgresExecutor, sql
from mayim.exception import MissingSQL, RecordNotFound


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
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%(item_id)s", {"item_id": 999}
    )


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


async def test_empty_result_none_optional(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @sql("SELECT * FROM otheritems")
        async def select_items(self) -> Optional[int]:
            ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    result = await executor.select_items()
    assert result is None


async def test_empty_result_none_optional_list(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @sql("SELECT * FROM otheritems")
        async def select_items(self) -> Optional[List[int]]:
            ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    result = await executor.select_items()
    assert result is None


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="Requires 3.10 style annotations"
)
async def test_empty_result_none_union(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @sql("SELECT * FROM otheritems")
        async def select_items(self) -> int | None:
            ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    result = await executor.select_items()
    assert result is None


def test_missing_sql_not_strict():
    class FooExecutor(PostgresExecutor):
        async def select_missing(self) -> int:
            ...

    Mayim(executors=[FooExecutor()], dsn="foo://user:password@host:1234/db")


def test_missing_sql_strict():
    class FooExecutor(PostgresExecutor):
        async def select_missing(self) -> int:
            ...

    message = re.escape(
        "Could not find SQL for FooExecutor.select_missing. "
        "Looked for file named: "
    )
    with pytest.raises(MissingSQL, match=message):
        Mayim(
            executors=[FooExecutor()],
            dsn="foo://user:password@host:1234/db",
            strict=True,
        )
