import re
import sys
from dataclasses import asdict

import pytest

from mayim import Mayim, PostgresExecutor, query
from mayim.exception import MissingSQL, RecordNotFound


@pytest.mark.parametrize(
    "method_name",
    (
        "select_otheritem",
        "select_otheritem_execute",
        "select_otheritem_run_sql",
    ),
)
async def test_returns_single_item(
    postgres_connection, item_executor, method_name, Item
):
    postgres_connection.result = {"item_id": 999, "name": "FooBar"}
    method = getattr(item_executor, method_name)
    result = await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%(item_id)s", {"item_id": 999}
    )
    if method_name.endswith("run_sql"):
        assert isinstance(result, dict)
        assert result == {"item_id": 999, "name": "FooBar"}
    else:
        assert isinstance(result, Item)
        assert asdict(result) == {"item_id": 999, "name": "FooBar"}


@pytest.mark.parametrize(
    "method_name", ("select_otheritem", "select_otheritem_execute")
)
async def test_empty_result_single(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = None
    query_name = (
        f"<{method_name}> " if not method_name.endswith("execute") else ""
    )
    message = re.escape(
        f"Query {query_name}did not find any record using "
        "() and {'item_id': 999}"
    )
    method = getattr(item_executor, method_name)
    with pytest.raises(RecordNotFound, match=message):
        await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%(item_id)s", {"item_id": 999}
    )


@pytest.mark.parametrize("method_name", ("select_int", "select_int_execute"))
async def test_returns_single_int(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = {"item_id": 999}
    method = getattr(item_executor, method_name)
    result = await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%(item_id)s", {"item_id": 999}
    )
    assert isinstance(result, int)
    assert result == 999


@pytest.mark.parametrize(
    "method_name", ("select_otheritems", "select_otheritems_execute")
)
async def test_returns_multiple_item(
    postgres_connection, item_executor, method_name, Item
):
    postgres_connection.result = [
        {"item_id": 999, "name": "FooBar"},
        {"item_id": 888, "name": "BarFoo"},
    ]
    method = getattr(item_executor, method_name)
    result = await method()
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems", None
    )
    assert all(isinstance(item, Item) for item in result)
    assert asdict(result[0]) == {"item_id": 999, "name": "FooBar"}
    assert asdict(result[1]) == {"item_id": 888, "name": "BarFoo"}


@pytest.mark.parametrize(
    "method_name", ("select_otheritems", "select_otheritems_execute")
)
async def test_empty_result_multiple(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = None
    method = getattr(item_executor, method_name)
    result = await method()
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems", None
    )
    assert result == []


@pytest.mark.parametrize(
    "method_name", ("select_optional_item", "select_optional_item_execute")
)
async def test_empty_result_optional_item(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = None
    method = getattr(item_executor, method_name)
    result = await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%(item_id)s", {"item_id": 999}
    )
    assert result is None


@pytest.mark.parametrize(
    "method_name", ("select_optional_items", "select_optional_items_execute")
)
async def test_empty_result_none_optional_list(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = None
    method = getattr(item_executor, method_name)
    result = await method()
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems", None
    )
    assert result is None


@pytest.mark.parametrize(
    "method_name",
    (
        "update_item_empty",
        "update_item_none",
        "update_item_empty_execute",
        "update_item_none_execute",
    ),
)
async def test_no_return_annotation(
    postgres_connection, item_executor, method_name
):
    postgres_connection.result = None
    method = getattr(item_executor, method_name)
    result = await method(item_id=999, name="foo")
    postgres_connection.execute.assert_called_with(
        "UPDATE otheritems SET name=%(name)s WHERE item_id=%(item_id)s",
        {"item_id": 999, "name": "foo"},
    )
    assert result is None


@pytest.mark.parametrize(
    "method_name",
    ("select_otheritem_positional", "select_otheritem_positional_execute"),
)
async def test_returns_single_item_positional(
    postgres_connection, item_executor, method_name, Item
):
    postgres_connection.result = {"item_id": 999, "name": "FooBar"}
    method = getattr(item_executor, method_name)
    result = await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT * FROM otheritems WHERE item_id=%s", [999]
    )
    assert isinstance(result, Item)
    assert asdict(result) == {"item_id": 999, "name": "FooBar"}


@pytest.mark.parametrize(
    "method_name",
    ("select_item", "select_item_named"),
)
async def test_run_sql(postgres_connection, item_executor, method_name):
    postgres_connection.result = {"item_id": 999, "name": "FooBar"}
    method = getattr(item_executor, method_name)
    result = await method(item_id=999)
    postgres_connection.execute.assert_called_with(
        "SELECT *\nFROM items\nWHERE item_id = %(item_id)s\n", {"item_id": 999}
    )
    assert isinstance(result, dict)
    assert result == {"item_id": 999, "name": "FooBar"}


@pytest.mark.skipif(
    sys.version_info < (3, 10), reason="Requires 3.10 style annotations"
)
async def test_empty_result_none_union(postgres_connection):
    postgres_connection.result = None

    class ItemExecutor(PostgresExecutor):
        @query("SELECT * FROM otheritems")
        async def select_otheritems(self) -> int | None: ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    result = await executor.select_otheritems()
    assert result is None


def test_missing_sql_not_strict():
    class FooExecutor(PostgresExecutor):
        async def select_missing(self) -> int: ...

    Mayim(
        executors=[FooExecutor()],
        dsn="foo://user:password@host:1234/db",
        strict=False,
    )


def test_missing_sql_strict():
    class FooExecutor(PostgresExecutor):
        async def select_missing(self) -> int: ...

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
