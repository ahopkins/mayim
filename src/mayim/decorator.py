from ast import (
    AsyncFunctionDef,
    Constant,
    Expr,
    FunctionDef,
    Pass,
    parse,
)
from functools import wraps
from inspect import (
    cleandoc,
    getdoc,
    getmodule,
    getsource,
    signature,
)
from logging import getLogger
from textwrap import dedent
from typing import get_args, get_origin
from mayim.exception import MayimError


logger = getLogger("booktracker")


def is_auto_exec(func):
    src = dedent(getsource(func))
    tree = parse(src)

    assert isinstance(tree.body[0], (FunctionDef, AsyncFunctionDef))
    body = tree.body[0].body

    return len(body) == 1 and (
        (
            isinstance(body[0], Expr)
            and isinstance(body[0].value, Constant)
            and (
                body[0].value.value is Ellipsis
                or (
                    isinstance(body[0].value.value, str)
                    and cleandoc(body[0].value.value) == cleandoc(getdoc(func))
                )
            )
        )
        or isinstance(body[0], Pass)
    )


def execute(func):
    """
    Responsible for executing a DB query and passing the result off to a
    hydrator.

    If the Executor does not contain any code, then the assumption is that
    we should automatically execute the in memory SQL, and passing the results
    off to the base Hydrator.
    """
    sig = signature(func)

    # TODO
    # - Make sure that this is the ONLY part of the source, and also
    #   accept methods that only have a docstring and no code.
    auto_exec = is_auto_exec(func)
    model = sig.return_annotation
    as_list = False
    name = func.__name__

    if model is not None and (origin := get_origin(model)):
        as_list = bool(origin is list)
        if not as_list:
            return MayimError(
                f"{func} must return either a model or a list of models. "
                "eg. -> Foo or List[Foo]"
            )
        model = get_args(model)[0]

    def decorator(f):
        @wraps(f)
        async def decorated_function(*args, **kwargs):
            self = args[0]
            if auto_exec:
                query = self._queries[name]
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                values = {**bound.arguments}
                values.pop("self", None)

                results = await self._execute(
                    query,
                    model=model,
                    as_list=as_list,
                    _convert=False,
                    **values,
                )

                if model is None:
                    return None
            else:
                self._context.set((model, name))
                results = await f(*args, **kwargs)

            return results

        return decorated_function

    return decorator(func)


def sql(query: str):
    def decorator(f):
        name = f.__name__

        # @wraps(f)
        # async def decorated_function(*args, **kwargs):
        #     if auto_exec:
        #         self = args[0]
        #         query = self._queries[name]

        class_name = f.__qualname__.split(".")[0]
        # raise Exception(Executor._registry[class_name])
        module = getmodule(f)
        # raise Exception(getattr(module, class_name))
        raise Exception(f.__globals__.keys())

    return decorator
