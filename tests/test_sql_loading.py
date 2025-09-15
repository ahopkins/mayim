from mayim import Mayim, PostgresExecutor, query
from mayim.sql.postgres.query import PostgresQuery
from mayim.sql.query import ParamType, SQLQuery

from .app.model import Item

EXPECTED_KEYWORD = PostgresQuery(
    "expected",
    """SELECT *
FROM items
LIMIT %(limit)s OFFSET %(offset)s;
""",
)
EXPECTED_POSITIONAL = PostgresQuery(
    "expected",
    """SELECT *
FROM items
LIMIT %s OFFSET %s;
""",
)


async def test_auto_load_keyword(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Item: ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    assert ItemExecutor._queries["select_items"] == EXPECTED_KEYWORD
    assert (
        ItemExecutor._queries["select_items"].param_type is ParamType.KEYWORD
    )
    await executor.select_items()

    postgres_connection.execute.assert_called_with(
        EXPECTED_KEYWORD.text, {"limit": 4, "offset": 0}
    )


async def test_sql_decorator_keyword(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        @query(
            """SELECT *
            FROM otheritems
            LIMIT $limit OFFSET $offset;
            """
        )
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Item: ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    assert ItemExecutor._queries["select_items"] != EXPECTED_KEYWORD
    assert (
        ItemExecutor._queries["select_items"].param_type is ParamType.KEYWORD
    )
    await executor.select_items(limit=10, offset=40)

    query_text = EXPECTED_KEYWORD.text.replace("items", "otheritems").strip()
    postgres_connection.execute.assert_called_with(
        query_text, {"limit": 10, "offset": 40}
    )


async def test_auto_load_positional(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        async def select_items_numbered(
            self, limit: int = 4, offset: int = 0
        ) -> Item: ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    assert (
        ItemExecutor._queries["select_items_numbered"] == EXPECTED_POSITIONAL
    )
    assert (
        ItemExecutor._queries["select_items_numbered"].param_type
        is ParamType.POSITIONAL
    )
    await executor.select_items_numbered()

    postgres_connection.execute.assert_called_with(
        EXPECTED_POSITIONAL.text, [4, 0]
    )


async def test_sql_decorator_positional(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        @query(
            """SELECT *
            FROM otheritems
            LIMIT $1 OFFSET $2;
            """
        )
        async def select_items_numbered(
            self, limit: int = 4, offset: int = 0
        ) -> Item: ...

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)

    assert (
        ItemExecutor._queries["select_items_numbered"] != EXPECTED_POSITIONAL
    )
    assert (
        ItemExecutor._queries["select_items_numbered"].param_type
        is ParamType.POSITIONAL
    )
    await executor.select_items_numbered(limit=10, offset=40)

    query_text = EXPECTED_POSITIONAL.text.replace(
        "items", "otheritems"
    ).strip()
    postgres_connection.execute.assert_called_with(query_text, [10, 40])


async def test_get_query_by_name():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> SQLQuery:
            return self.get_query("select_items")

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)
    query = await executor.select_items()

    assert isinstance(query, PostgresQuery)
    assert query.name == "select_items"


async def test_get_query_by_call_stack():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> SQLQuery:
            return self.get_query()

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)
    query = await executor.select_items()

    assert isinstance(query, PostgresQuery)
    assert query.name == "select_items"
