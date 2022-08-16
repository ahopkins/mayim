from inspect import cleandoc

from mayim.base.hydrator import Hydrator
from mayim.registry import LazyHydratorRegistry, LazyQueryRegistry, Registry


def query(query: str):
    """Convenience decorator to supply a query to an executor method without
    loading if from a source file.

    Example:

    ```python
    from mayim import PostgresExecutor, query

    class ItemExecutor(PostgresExecutor):
        @query(
            '''
            SELECT *
            FROM items
            WHERE item_id = $item_id;
            '''
        )
        async def select_item(self, item_id: int) -> Item:
            ...
    ```

    Args:
        query (str): The query
    """

    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazyQueryRegistry.add(class_name, method_name, cleandoc(query))
        return f

    return decorator


def hydrator(hydrator: Hydrator):
    """Convenience decorator to supply a specific hydrator to an
    executor method.

    Example:

    ```python
    from mayim import Mayim, Executor, Hydrator, hydrator

    class HydratorA(Hydrator):
        ...

    class HydratorB(Hydrator):
        ...

    class SomeExecuto(Executor):
        async def select_a(...) -> Something:
            ...

        @hydrator(HydratorB())
        async def select_b(...) -> Something:
            ...

    Mayim(executors=[SomeExecutor(hydrator=HydratorA())])
    ```

    Args:
        hydrator (Hydrator): The hydrator
    """

    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazyHydratorRegistry.add(class_name, method_name, hydrator)
        return f

    return decorator


def register(cls):
    """Convenience decorator to preregister an executor

    Example:

    ```python
    from mayim import PostgresExecutor, register

    @register
    class MyExecutor(PostgresExecutor):
        async def select_something(self) -> Something:
            ...
    ```
    """
    Registry().register(cls)
    return cls
