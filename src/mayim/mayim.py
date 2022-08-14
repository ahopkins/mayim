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
        strict: Optional[bool] = None,
    ) -> None:
        """
        Look through the Executor definition for methods that should
        execute a query, then load the corresponding SQL from a .sql file
        and hold in memory.
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
        for interface in InterfaceRegistry():
            await interface.close()
