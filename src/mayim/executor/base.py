from __future__ import annotations

from ast import AsyncFunctionDef, Constant, Expr, FunctionDef, Pass, parse
from contextvars import ContextVar
from inspect import cleandoc, getdoc, getsource
from textwrap import dedent
from typing import Dict, Optional, Tuple, Type

from mayim.hydrator import Hydrator
from mayim.interface.base import BaseInterface
from mayim.interface.lazy import LazyPool
from mayim.registry import Registry


class Executor:
    """
    Responsible for being the interface between the DB and the application
    """

    _queries: Dict[str, str]
    _fallback_hydrator: Hydrator
    _fallback_pool: Optional[BaseInterface]
    _loaded: bool = False

    def __init__(
        self,
        pool: Optional[BaseInterface] = None,
        hydrator: Optional[Hydrator] = None,
    ) -> None:
        pool = pool or getattr(self.__class__, "_fallback_pool", None)
        if not pool:
            pool = LazyPool()
        self._pool = pool
        self._hydrator = hydrator
        self._context: ContextVar[Tuple[Type[object], str]] = ContextVar(
            "_context"
        )
        Registry().register(self)

    @property
    def hydrator(self) -> Hydrator:
        if self._hydrator:
            return self._hydrator
        return self._fallback_hydrator

    @property
    def pool(self) -> BaseInterface:
        return self._pool

    def execute(
        self,
        query: str,
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        **values,
    ):

        raise NotImplementedError(
            f"{self.__class__.__name__} does not define execute"
        )

    def run_sql(
        self,
        query: str = "",
        as_list: bool = False,
        **values,
    ):
        raise NotImplementedError(
            f"{self.__class__.__name__} does not define run_sql"
        )

    @staticmethod
    def setup(func):
        return func


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
