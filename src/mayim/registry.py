from __future__ import annotations

from collections import defaultdict
from inspect import isclass
from typing import TYPE_CHECKING, DefaultDict, Dict, Optional, Type, Union

if TYPE_CHECKING:
    from mayim.executor import Executor
    from mayim.hydrator import Hydrator


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


class LazySQLRegistry:
    _singleton = None
    _queries: DefaultDict[str, Dict[str, str]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._queries = defaultdict(dict)
        return cls._singleton

    @classmethod
    def add(cls, class_name: str, method_name: str, query: str) -> None:
        instance = cls()
        instance._queries[class_name][method_name] = query

    @classmethod
    def get(cls, class_name: str, method_name: str) -> Optional[str]:
        return cls()._queries.get(class_name, {}).get(method_name, None)


class LazyHydratorRegistry:
    _singleton = None
    _hydrators: DefaultDict[str, Dict[str, Hydrator]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._hydrators = defaultdict(dict)
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
