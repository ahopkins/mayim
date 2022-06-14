from inspect import cleandoc

from mayim.hydrator.base import Hydrator
from mayim.registry import LazyHydratorRegistry, LazySQLRegistry, Registry


def sql(query: str):
    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazySQLRegistry.add(class_name, method_name, cleandoc(query))
        return f

    return decorator


def hydrator(hydrator: Hydrator):
    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazyHydratorRegistry.add(class_name, method_name, hydrator)
        return f

    return decorator


def register(cls):
    Registry().register(cls)
    return cls
