from __future__ import annotations

from collections import defaultdict
from inspect import isclass
from typing import TYPE_CHECKING, DefaultDict, Dict, Optional, Set, Type, Union

if TYPE_CHECKING:
    from mayim.base import Executor, Hydrator
    from mayim.base.interface import BaseInterface


class Registry(dict):
    _singleton = None

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    def register(self, executor: Union[Type[Executor], Executor]) -> None:
        cls = executor if isclass(executor) else executor.__class__
        if cls not in self:
            self[cls.__name__] = executor

    @classmethod
    def reset(cls):
        cls._singleton = super().__new__(cls)  # type: ignore


class InterfaceRegistry:
    _singleton = None
    _interfaces: Set[BaseInterface]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    @classmethod
    def add(cls, interface: BaseInterface) -> None:
        instance = cls()
        instance._interfaces.add(interface)

    def __iter__(self):
        return iter(self._interfaces)

    @classmethod
    def reset(cls):
        cls._singleton = super().__new__(cls)
        cls._singleton._interfaces = set()


class LazyQueryRegistry:
    _singleton = None
    _queries: DefaultDict[str, Dict[str, str]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    @classmethod
    def add(cls, class_name: str, method_name: str, query: str) -> None:
        instance = cls()
        instance._queries[class_name][method_name] = query

    @classmethod
    def get(cls, class_name: str, method_name: str) -> Optional[str]:
        return cls()._queries.get(class_name, {}).get(method_name, None)

    @classmethod
    def reset(cls):
        cls._singleton = super().__new__(cls)
        cls._singleton._queries = defaultdict(dict)


class LazyHydratorRegistry:
    _singleton = None
    _hydrators: DefaultDict[str, Dict[str, Hydrator]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    @classmethod
    def add(
        cls, class_name: str, method_name: str, hydrator: Hydrator
    ) -> None:
        instance = cls()
        instance._hydrators[class_name][method_name] = hydrator

    @classmethod
    def get(cls, class_name: str, method_name: str) -> Optional[Hydrator]:
        return cls()._hydrators.get(class_name, {}).get(method_name, None)

    @classmethod
    def reset(cls):
        cls._singleton = super().__new__(cls)
        cls._singleton._hydrators = defaultdict(dict)


class PoolRegistry:
    """
    Registry to ensure executors with the same DSN share the same pool instance.
    This prevents duplicate connections and enables proper transaction coordination.
    """

    _singleton = None
    _pools: Dict[str, BaseInterface]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    @classmethod
    def get_or_create(
        cls,
        dsn: str,
        pool_class: Type[BaseInterface],
        min_size: int = 1,
        max_size: Optional[int] = None,
    ) -> BaseInterface:
        """
        Get existing pool or create new one for DSN.

        Args:
            dsn: Database connection string
            pool_class: Class to use for creating new pool
            min_size: Minimum number of connections in pool
            max_size: Maximum number of connections in pool

        Returns:
            Shared pool instance for the DSN
        """
        instance = cls()
        if dsn not in instance._pools:
            instance._pools[dsn] = pool_class(
                dsn, min_size=min_size, max_size=max_size
            )
        return instance._pools[dsn]

    @classmethod
    def get(cls, dsn: str) -> Optional[BaseInterface]:
        """Get pool for DSN if it exists"""
        instance = cls()
        return instance._pools.get(dsn)

    @classmethod
    def reset(cls):
        """Reset the registry (useful for testing)"""
        cls._singleton = super().__new__(cls)
        cls._singleton._pools = {}
