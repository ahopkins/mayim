from mayim import Mayim, PostgresExecutor, sql
from mayim.query.postgres import PostgresQuery
from .app.model import Item


EXPECTED = PostgresQuery(
    """SELECT *
FROM items
LIMIT %(limit)s OFFSET %(offset)s;
"""
)


async def test_auto_load(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        async def select_items(self, limit: int = 4, offset: int = 0) -> Item:
            ...

    Mayim(executors=[ItemExecutor], dsn="fake")
    executor = Mayim.get(ItemExecutor)

    assert ItemExecutor._queries["select_items"] == EXPECTED
    await executor.select_items()

    postgres_connection.execute.assert_called_with(
        EXPECTED.text, {"limit": 4, "offset": 0}
    )


async def test_sql_decorator(postgres_connection):
    postgres_connection.result = {"item_id": 99, "name": "thing"}

    class ItemExecutor(PostgresExecutor):
        @sql(
            """SELECT *
            FROM otheritems
            LIMIT $limit OFFSET $offset;
            """
        )
        async def select_items(self, limit: int = 4, offset: int = 0) -> Item:
            ...

    Mayim(executors=[ItemExecutor], dsn="fake")
    executor = Mayim.get(ItemExecutor)

    assert ItemExecutor._queries["select_items"] != EXPECTED
    await executor.select_items(limit=10, offset=40)

    query_text = EXPECTED.text.replace("items", "otheritems").strip()
    postgres_connection.execute.assert_called_with(
        query_text, {"limit": 10, "offset": 40}
    )
