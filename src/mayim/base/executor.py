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

from mayim.base.hydrator import Hydrator
from mayim.base.interface import BaseInterface
from mayim.base.query import Query
from mayim.exception import MayimError
from mayim.lazy.interface import LazyPool
from mayim.registry import Registry

T = TypeVar("T", bound=Query)


class Executor(Generic[T]):
    """
    Base class for creating executors, which serve as the main interface for
    interacting with a data source. Likely you will want to create a subclass
    from one of its subclasses and not directly from this base class.
    """

    _queries: Dict[str, T]
    _fallback_hydrator: Hydrator
    _fallback_pool: Optional[BaseInterface]
    _loaded: bool = False
    _hydrators: Dict[str, Hydrator]
    path: Optional[Union[str, Path]] = None
    """`Optional[Union[str, Path]]`: Class property that is a custom  path "
        location of queries to be loaded. Default to `None`"""
    ENABLED: bool = True
    QUERY_CLASS: Type[T]

    def __init__(
        self,
        pool: Optional[BaseInterface] = None,
        hydrator: Optional[Hydrator] = None,
    ) -> None:
        """Base class for creating executors

        Args:
            pool (BaseInterface, optional): An interface used
                for a specific executor to override a global pool.
                Defaults to `None`.
            hydrator (Hydrator, optional): A hydrator used
                for a specific executor to override a global hydrator.
                Defaults to `None`.

        Raises:
            MayimError: If a dependency is missing
        """
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
        """The assigned hydrator. Will return an instance specific hydrator
        if one was assigned.

        Returns:
            Hydrator: The hydrator
        """
        if self._hydrator:
            return self._hydrator
        return self._fallback_hydrator

    @property
    def pool(self) -> BaseInterface:
        """The assigned pool. Will return an instance specific pool
        if one was assigned.

        Returns:
            BaseInterface: The pool interface
        """
        return self._pool

    def execute(
        self,
        query: Union[str, Query],
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        allow_none: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        """Low-level API to execute a query and hydrate the results

        Args:
            query (Union[str, Query]): The query to be executed
            name (str, optional): The name of the query. Defaults to `""`.
            model (Type[object], optional): The model to be used
                for hydration. Defaults to `None`.
            as_list (bool, optional): Whether to return the results as a
                list of hydrated objects. Defaults to `False`.
            allow_none (bool, optional): Whether `None` is an acceptable return
                value. Defaults to `False`.
            posargs (Sequence[Any], optional): Positional arguments.
                Defaults to `None`.
            params (Dict[str, Any], optional): Keyword arguments.
                Defaults to `None`.
        """
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
        """Low-level API to execute a query and return the results without
        hydration.

        Args:
            query (Union[str, Query]): The query to be executed
            name (str, optional): The name of the query. Defaults to `""`.
            as_list (bool, optional): Whether to return the results as a
                list of hydrated objects. Defaults to `False`.
            allow_none (bool, optional): Whether `None` is an acceptable return
                value. Defaults to `False`.
            posargs (Sequence[Any], optional): Positional arguments.
                Defaults to `None`.
            params (Dict[str, Any], optional): Keyword arguments.
                Defaults to `None`.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not define run_sql"
        )

    def get_query(self, name: Optional[str] = None) -> T:
        """Return a query

        Args:
            name (str, optional): The name of the query to be
                retrieved. If no name is supplied, it will look for the query
                by the name of the calling method. Defaults to `None`.

        Raises:
            MayimError: When the query coul not be found

        Returns:
            Query: A query object
        """
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
        """Return a hydrator

        Args:
            name (str, optional): The name of the hydrator to be
                retrieved. If no name is supplied, it will look for the
                hydrator by the name of the calling method. Defaults to `None`.

        Raises:
            MayimError: When the hydrator coul not be found

        Returns:
            Hydrator: A hydrator object
        """
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
    def _load(cls, strict: bool) -> None:
        ...

    @staticmethod
    def is_query_name(obj) -> bool:
        """Whether an object is a valid query name

        Args:
            obj (Any): Depends upon which subclass is being implemented

        Returns:
            bool: is it valid
        """
        return False

    @staticmethod
    def _setup(func):
        return func

    @classmethod
    def get_base_path(cls, directory_name: Optional[str]) -> Path:
        """Get the base path for where queries will be located

        Args:
            directory_name (str, optional): A starting directory

        Raises:
            MayimError: When a module does not exist

        Returns:
            Path: The base path
        """
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


def is_auto_exec(func) -> bool:
    """Check if a method should be auto-executed.

    Example:

    Any of the following are acceptable:

    ```python
    async def method_ellipsis(self) -> None:
        ...

    async def method_pass(self) -> None:
        pass

    async def method_docstring(self) -> None:
        '''This is a docstring'''
    ```

    Args:
        func: The function or method being checked

    Returns:
        bool: Whether the function is empty and should be auto-executed
    """
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
                    and cleandoc(body[0].value.value)
                    == cleandoc(getdoc(func) or "")
                )
            )
        )
        or isinstance(body[0], Pass)
    )
