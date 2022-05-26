from inspect import isclass
from typing import Optional, Sequence, Type, TypeVar, Union

from mayim.exception import MayimError
from mayim.executor import Executor
from mayim.hydrator import Hydrator
from mayim.interface.base import BaseInterface
from mayim.interface.lazy import LazyPool
from mayim.interface.postgres import PostgresPool
from mayim.registry import Registry

T = TypeVar("T", bound=Executor)


class Mayim:
    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ):
        if pool and dsn:
            raise MayimError("Conflict with pool and DSN")

        if not pool and dsn:
            pool = PostgresPool(dsn)

        if not executors:
            executors = []
        self.load(executors=executors, hydrator=hydrator, pool=pool)

        if hydrator is None:
            hydrator = Hydrator()

        Executor._fallback_hydrator = hydrator
        Executor._fallback_pool = pool

    @staticmethod
    def get(executor: Type[T]) -> T:
        registry = Registry()
        try:
            instance = registry[executor.__name__]
        except KeyError as e:
            raise MayimError(f"{executor} has not been registered") from e
        return instance

    def register(cls, executor: Executor) -> None:
        Registry().register(executor)

    def reset_registry(self):
        Registry.reset()

    def load(
        self,
        *,
        executors: Sequence[Union[Type[Executor], Executor]],
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ) -> None:
        """
        Look through the Executor definition for methods that should
        execute a query, then load the corresponding SQL from a .sql file
        and hold in memory.
        """

        to_load = set(executors)
        to_load.update(Registry().values())
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

            executor._load()
