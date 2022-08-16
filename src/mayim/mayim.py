from asyncio import get_running_loop
from inspect import isclass
from typing import Optional, Sequence, Type, TypeVar, Union
from urllib.parse import urlparse

from mayim.base import Executor, Hydrator
from mayim.base.interface import BaseInterface
from mayim.exception import MayimError
from mayim.lazy.interface import LazyPool
from mayim.registry import InterfaceRegistry, Registry
from mayim.sql.postgres.interface import PostgresPool

T = TypeVar("T", bound=Executor)
DEFAULT_INTERFACE = PostgresPool


class Mayim:
    """Main entryway for initializing access to the data layer.

    Where possible, it is advised (although not necessary) to initialize the
    Mayim object once the async loop is running.

    Example:

    ```python
    async def run():
        Mayim(
            executors=[MyExecutor]
            dsn="postgres://user:pass@localhost:5432/mydb"
        )
    ```
    """

    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        db_path: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        strict: bool = True,
    ):
        """Initializer for Mayim instance

        The `dsn`, `db_path`, and the `pool` are mutually exclusive. Either
        one or none of them should be passed. If one exists, it will be used
        to create the fallback interface pool for the datasource. Therefore,
        if an executor does not define its own, then it will use the fallback
        pool as established here.

        Similarly, the `hydrator` is used in the case that an executor does
        not define a hydrator. If one is not provided, then a generic hydrator
        instance will be created.

        Executors need not be registered when the `Mayim` instance is
        instantiated. See
        [load](./mayim.mayim.html#load) and [loading and instantiating](/guide/executors.html#loading-and-instantiating)
        for more details.

        Args:
            executors (Sequence[Union[Type[Executor], Executor]], optional): Executors
                that are being loaded at initialization time.
                Defaults to `None`.
            dsn (str, optional): DSN to the data source. Defaults to `""`.
            db_path (str, optional): Path to a SQLite database.
                Defaults to `""`.
            hydrator (Hydrator, optional): Fallback hydrator to use if not
                specified. Defaults to `None`.
            pool (BaseInterface, optional): Fallback database interface.
                Defaults to `None`.
            strict (bool, optional): Whether to raise an error if there is
                an empty method in a decorator but no loaded query.
                Defaults to `True`.

        Raises:
            MayimError: If there is conflicing data access source
        """  # noqa
        if pool and dsn:
            raise MayimError("Conflict with pool and DSN")

        if dsn and db_path:
            raise MayimError("Conflict with DSN and DB path")

        if db_path and not dsn:
            dsn = db_path

        if not pool and dsn:
            pool_type = self._get_pool_type(dsn)
            try:
                get_running_loop()
            except RuntimeError:
                pool = LazyPool()
                pool.set_derivative(pool_type)
                pool.set_dsn(dsn)
            else:
                pool = pool_type(dsn)

        if not executors:
            executors = []
        self.strict = strict
        self.load(executors=executors, hydrator=hydrator, pool=pool)

        if hydrator is None:
            hydrator = Hydrator()

        Executor._fallback_hydrator = hydrator
        Executor._fallback_pool = pool

    @staticmethod
    def get(executor: Type[T]) -> T:
        """Fetch an instance of an executor

        This is useful when there you wish to retrieve the registered instance
        of an executor. Can be used in various parts of
        an application without having to pass variables.

        Example:

        ```python
        from mayim import Mayim
        from my.package.executors import AwesomeExecutor

        async def some_func():
            executor = Mayim.get(AwesomeExecutor)
            awesomeness = await executor.select_something_awesome()
            ...
        ```

        Args:
            executor (Type[T]): The class of the registered executor instance

        Raises:
            MayimError: If the passed executor has not been registered

        Returns:
            Executor: The executor instance
        """
        registry = Registry()
        try:
            instance = registry[executor.__name__]
        except KeyError as e:
            raise MayimError(f"{executor} has not been registered") from e
        return instance

    def register(cls, executor: Executor) -> None:
        """Register an Executor instance

        Args:
            executor (Executor): The instance to be registered
        """
        Registry().register(executor)

    def reset_registry(self):
        """Empty the executor registry"""
        Registry.reset()

    def load(
        self,
        *,
        executors: Sequence[Union[Type[Executor], Executor]],
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        strict: Optional[bool] = None,
    ) -> None:
        """Look through the Executor definition for methods that should
        execute a query, then load the corresponding SQL from a .sql file
        and hold in memory.

        Args:
            executors (Sequence[Union[Type[Executor], Executor]]): The
                executor classes or instances to be registered
            hydrator (Hydrator, optional): Fallback hydrator to use if not
                specified. Defaults to `None`.
            pool (BaseInterface, optional): Fallback database interface.
                Defaults to `None`.
            strict (bool, optional): Whether to raise an error if there is
                an empty method in a decorator but no loaded query.
                Defaults to `True`.

        Raises:
            MayimError: If there is no fallback database interface, Mayim will
                raise an exception if there is a registered executor without
                an instance-specific database interface
        """
        to_load = set(executors)
        to_load.update(Registry().values())
        strict = strict if strict is not None else self.strict
        for executor in to_load:
            if isclass(executor):
                try:
                    executor = self.get(executor)
                except MayimError:
                    ...
            if isinstance(executor, Executor):
                if executor.pool is LazyPool():
                    if not pool:
                        raise MayimError(
                            f"Cannot load {executor} without a pool"
                        )
                    executor._pool = pool
                executor = executor.__class__
            else:
                executor(pool=pool, hydrator=hydrator)

            if executor._loaded:
                continue

            executor._load(strict)

    def _get_pool_type(self, dsn: str) -> Type[BaseInterface]:
        parts = urlparse(dsn)

        for interface_type in BaseInterface.registered_interfaces:
            if parts.scheme == interface_type.scheme:
                return interface_type
        return DEFAULT_INTERFACE

    async def connect(self) -> None:
        """Connect to all database interfaces"""
        registry = Registry()
        to_derive = {
            executor
            for executor in registry.values()
            if isinstance(executor.pool, LazyPool)
        }
        for executor in to_derive:
            derived = executor.pool.derive()
            executor._pool = derived
            if isinstance(executor.__class__._fallback_pool, LazyPool):
                executor.__class__._fallback_pool = derived

        for interface in InterfaceRegistry():
            await interface.open()

    async def disconnect(self) -> None:
        """Disconnect from all database interfaces"""
        for interface in InterfaceRegistry():
            await interface.close()
