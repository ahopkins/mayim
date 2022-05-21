from inspect import getmembers, getmodule, isclass, isfunction
from pathlib import Path
from typing import Optional, Sequence, Type, TypeVar, Union

from mayim.convert import convert_sql_params
from mayim.exception import MayimError
from mayim.executor import Executor, is_auto_exec
from mayim.hydrator import Hydrator
from mayim.interface.base import BaseInterface
from mayim.interface.lazy import LazyPool
from mayim.interface.postgres import PostgresPool
from mayim.registry import LazySQLRegistry, Registry

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

            executor._queries = {}
            module = getmodule(executor)
            if not module or not module.__file__:
                raise MayimError(f"Could not locate module for {executor}")

            base = Path(module.__file__).parent

            for name, func in getmembers(executor, self.isoperation):
                auto_exec = is_auto_exec(func)
                setattr(executor, name, executor.setup(func))

                query = LazySQLRegistry.get(executor.__name__, name)
                path = base / "queries" / f"{name}.sql"

                try:
                    executor._queries[name] = self.load_sql(query, path)
                except FileNotFoundError:
                    if auto_exec:
                        ...
            executor._loaded = True

    @staticmethod
    def isoperation(obj):
        """Check if the object is a method that starts with get_ or create_"""
        if isfunction(obj):
            return (
                obj.__name__.startswith("select_")
                or obj.__name__.startswith("insert_")
                or obj.__name__.startswith("update_")
                or obj.__name__.startswith("delete_")
            )
        return False

    @staticmethod
    def load_sql(query: Optional[str], path: Path):
        if not query:
            with open(path, "r") as f:
                query = f.read()
        return convert_sql_params(query)
