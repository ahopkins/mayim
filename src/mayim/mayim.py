from typing import TYPE_CHECKING, Optional, Type, Union
from mayim.exception import MayimError
from mayim.executor import Executor

from mayim.hydrator import Hydrator
from mayim.interface.base import BaseInterface
from mayim.interface.psycopg import PostgresPool
from inspect import (
    getmembers,
    getmodule,
    getsourcelines,
    isawaitable,
    isclass,
    isfunction,
)
from pathlib import Path
import re
from typing import Dict, Optional, Tuple, Type, TypeVar, Union

from mayim.exception import MayimError
from mayim.interface.base import BaseInterface
from mayim.executor import Executor, Registry, CONVERT_PATTERN

from .decorator import execute

T = TypeVar("T", bound=Executor)


class Mayim:
    def __init__(
        self,
        *executors: Union[Type[Executor], Executor],
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ):
        if pool and dsn:
            raise MayimError("Conflict with pool and DSN")

        if not pool and dsn:
            pool = PostgresPool(dsn)
        self.load(*executors, hydrator=hydrator, pool=pool)

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
        *executors: Union[Type[Executor], Executor],
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ) -> None:
        """
        Look through the Executor definition for methods that should
        execute a query, then load the corresponding SQL from a .sql file
        and hold in memory.
        """
        for executor in set(executors):
            if isclass(executor):
                try:
                    executor = self.get(executor)
                except MayimError:
                    ...
            if isinstance(executor, Executor):
                executor = executor.__class__
            else:
                executor(pool=pool, hydrator=hydrator)
            executor._queries = {}
            module = getmodule(executor)
            if not module or not module.__file__:
                raise MayimError(f"Could not locate module for {executor}")

            base = Path(module.__file__).parent
            for name, func in getmembers(executor, self.isoperation):
                src = getsourcelines(func)
                auto_exec = src[0][-1].strip() in ("...", "pass")
                setattr(executor, name, execute(func))
                path = base / "queries" / f"{name}.sql"
                try:
                    executor._queries[name] = self.load_sql(path)
                except FileNotFoundError:
                    if auto_exec:
                        ...

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
    def load_sql(path: Path):
        with open(path, "r") as f:
            raw = f.read()
            return CONVERT_PATTERN.sub(r"%(\2)s", raw, 0)
