from mayim import Mayim, PostgresExecutor, hydrator
from mayim.base.hydrator import Hydrator


class HydratorA(Hydrator): ...


class HydratorB(Hydrator): ...


async def test_get_hydrator_by_name():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator("select_items")

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, Hydrator)
    assert instance.__class__.__name__ == "Hydrator"


async def test_get_hydrator_by_call_stack():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator()

    Mayim(executors=[ItemExecutor], dsn="foo://user:password@host:1234/db")
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, Hydrator)
    assert instance.__class__.__name__ == "Hydrator"


async def test_get_hydrator_by_name_executor():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator("select_items")

    Mayim(
        executors=[ItemExecutor(hydrator=HydratorA())],
        dsn="foo://user:password@host:1234/db",
    )
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, HydratorA)
    assert instance.__class__.__name__ == "HydratorA"


async def test_get_hydrator_by_call_stack_executor():
    class ItemExecutor(PostgresExecutor):
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator()

    Mayim(
        executors=[ItemExecutor(hydrator=HydratorA())],
        dsn="foo://user:password@host:1234/db",
    )
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, HydratorA)
    assert instance.__class__.__name__ == "HydratorA"


async def test_get_hydrator_by_name_method():
    class ItemExecutor(PostgresExecutor):
        @hydrator(HydratorB())
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator("select_items")

    Mayim(
        executors=[ItemExecutor(hydrator=HydratorA())],
        dsn="foo://user:password@host:1234/db",
    )
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, HydratorB)
    assert instance.__class__.__name__ == "HydratorB"


async def test_get_hydrator_by_call_stack_method():
    class ItemExecutor(PostgresExecutor):
        @hydrator(HydratorB())
        async def select_items(
            self, limit: int = 4, offset: int = 0
        ) -> Hydrator:
            return self.get_hydrator()

    Mayim(
        executors=[ItemExecutor(hydrator=HydratorA())],
        dsn="foo://user:password@host:1234/db",
    )
    executor = Mayim.get(ItemExecutor)
    instance = await executor.select_items()

    assert isinstance(instance, HydratorB)
    assert instance.__class__.__name__ == "HydratorB"
