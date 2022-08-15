from inspect import cleandoc

from mayim.base.hydrator import Hydrator
from mayim.registry import LazyHydratorRegistry, LazyQueryRegistry, Registry


def query(query: str):
    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazyQueryRegistry.add(class_name, method_name, cleandoc(query))
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
