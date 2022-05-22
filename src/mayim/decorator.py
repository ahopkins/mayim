from inspect import cleandoc

from mayim.registry import LazySQLRegistry, Registry


def sql(query: str):
    def decorator(f):
        *_, class_name, method_name = f.__qualname__.rsplit(".", 2)
        LazySQLRegistry.add(class_name, method_name, cleandoc(query))
        return f

    return decorator


def register(cls):
    Registry().register(cls)
    return cls
