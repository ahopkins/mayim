from __future__ import annotations

from ast import AsyncFunctionDef, Constant, Expr, FunctionDef, Pass, parse
from contextvars import ContextVar
from inspect import cleandoc, getdoc, getmodule, getsource, stack
from pathlib import Path
from textwrap import dedent
from typing import (
    Any,
    Dict,
    Generic,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from mayim.exception import MayimError
from mayim.hydrator import Hydrator
from mayim.interface.base import BaseInterface
from mayim.interface.lazy import LazyPool
from mayim.query.base import Query
from mayim.registry import Registry

T = TypeVar("T", bound=Query)


class Executor(Generic[T]):
    """
    Responsible for being the interface between the DB and the application
    """

    _queries: Dict[str, T]
    _fallback_hydrator: Hydrator
    _fallback_pool: Optional[BaseInterface]
    _loaded: bool = False
    _hydrators: Dict[str, Hydrator]
    path: Optional[Union[str, Path]] = None
    ENABLED: bool = True
    QUERY_CLASS: Type[T]

    def __init__(
        self,
        pool: Optional[BaseInterface] = None,
        hydrator: Optional[Hydrator] = None,
    ) -> None:
        if not self.ENABLED:
            raise MayimError(
                f"Cannot instantiate {self.__class__.__name__}. "
                "Perhaps you have a missing dependency?"
            )
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
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):

        raise NotImplementedError(
            f"{self.__class__.__name__} does not define execute"
        )

    def run_sql(
        self,
        query: str = "",
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        raise NotImplementedError(
            f"{self.__class__.__name__} does not define run_sql"
        )

    def get_query(self, name: Optional[str] = None) -> T:
        if not name:
            for frame in stack():
                if self.is_query_name(frame.function):
                    name = frame.function
                    break
            if not name:
                raise MayimError(
                    "Could not find query. Please specify a name."
                )
        return self._queries[name]

    def get_hydrator(self, name: Optional[str] = None) -> Hydrator:
        if not name:
            for frame in stack():
                if self.is_query_name(frame.function):
                    name = frame.function
                    break
            if not name:
                raise MayimError(
                    "Could not find hydrator. Please specify a name."
                )
        return self._hydrators.get(name, self.hydrator)

    @classmethod
    def _load(cls) -> None:
        ...

    @staticmethod
    def is_query_name(obj) -> bool:
        return False

    @staticmethod
    def _setup(func):
        return func

    @classmethod
    def get_base_path(cls, directory_name: Optional[str]) -> Path:
        module = getmodule(cls)
        if not module or not module.__file__:
            raise MayimError(f"Could not locate module for {cls}")

        base = Path(module.__file__).parent
        if cls.path is not None:
            if isinstance(cls.path, Path):
                return cls.path
            else:
                # TODO:
                # - support absolute path strings
                directory_name = cls.path
        if directory_name:
            base = base / directory_name
        return base


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
