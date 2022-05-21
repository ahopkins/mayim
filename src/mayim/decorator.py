from mayim.registry import LazySQLRegistry, Registry


def sql(query: str):
    def decorator(f):
        class_name, method_name = f.__qualname__.split(".", 1)
        LazySQLRegistry.add(class_name, method_name, query)
        return f

    return decorator


def register(cls):
    Registry().register(cls)
    return cls
