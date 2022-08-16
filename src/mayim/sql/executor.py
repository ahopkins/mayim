from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from functools import wraps
from inspect import Parameter, getmembers, isawaitable, isfunction, signature
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    Union,
    get_args,
    get_origin,
)

from mayim.base.executor import Executor, is_auto_exec
from mayim.base.query import Query
from mayim.convert import convert_sql_params
from mayim.exception import MayimError, MissingSQL, RecordNotFound
from mayim.lazy.interface import LazyPool
from mayim.registry import LazyHydratorRegistry, LazyQueryRegistry
from mayim.sql.query import ParamType, SQLQuery

if sys.version_info < (3, 10):  # no cov
    UnionType = type("UnionType", (), {})
else:
    from types import UnionType


class SQLExecutor(Executor[SQLQuery]):
    ENABLED: bool = False
    QUERY_CLASS = SQLQuery
    POSITIONAL_SUB: str = r"%s"
    KEYWORD_SUB: str = r"%(\2)s"
    generic_prefix: str = "mayim_"
    verb_prefixes: List[str] = [
        "select_",
        "insert_",
        "update_",
        "delete_",
    ]
    """Prefixes used to identify class methods and `.sql` files to load

    Example:

        ```python
        SQLExecutor.verb_prefixes = ["create_","read_","update_","delete_"]
        ```
    """

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
        if isinstance(query, Query):
            query = query.text
        query = convert_sql_params(
            query, self.POSITIONAL_SUB, self.KEYWORD_SUB
        )
        return self._execute(
            query=query,
            name=name,
            model=model,
            as_list=as_list,
            allow_none=allow_none,
            posargs=posargs,
            params=params,
        )

    async def _execute(
        self,
        query: str,
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        allow_none: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        no_result = False
        if model is None:
            model, _ = self._context.get()
        if model in (None, Parameter.empty):
            no_result = True
        hydrator = self._hydrators.get(name, self.hydrator)
        factory = hydrator._make(model)
        raw = await self._run_sql(
            query=query,
            name=name,
            as_list=as_list,
            no_result=no_result,
            posargs=posargs,
            params=params,
        )
        if no_result:
            return None
        if not raw:
            if allow_none:
                return None
            if as_list:
                return []
            query_name = f"<{name}> " if name else ""
            raise RecordNotFound(
                f"Query {query_name}did not find any record using "
                f"{posargs or ()} and {params or {}}"
            )
        results = factory(raw)
        if isawaitable(results):
            results = await results
        return results

    def run_sql(
        self,
        query: str = "",
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        if query:
            query = convert_sql_params(
                query, self.POSITIONAL_SUB, self.KEYWORD_SUB
            )
        else:
            if not name:
                _, query_name = self._context.get()
                name = query_name
            query = (self._queries[name]).text
        return self._run_sql(
            query=query,
            as_list=as_list,
            no_result=no_result,
            posargs=posargs,
            params=params,
        )

    async def _run_sql(
        self,
        query: str,
        name: str = "",
        as_list: bool = False,
        no_result: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        ...

    async def rollback(self) -> None:
        existing = self.pool.existing_connection()
        transaction = self.pool.in_transaction()
        if not existing or not transaction:
            raise MayimError("Cannot rollback non-existing transaction")
        await self._rollback(existing)

    async def _rollback(self, existing) -> None:
        self.pool._commit.set(False)
        await existing.rollback()

    def _get_method(self, as_list: bool) -> str:
        return "fetchall" if as_list else "fetchone"

    @asynccontextmanager
    async def transaction(self):
        self.pool._transaction.set(True)
        async with self.pool.connection() as conn:
            self.pool._connection.set(conn)
            try:
                yield
            except Exception:
                await self.rollback()
                raise
            finally:
                self.pool._connection.set(None)
                self.pool._transaction.set(False)
                self.pool._commit.set(True)

    @classmethod
    def _load(cls, strict: bool) -> None:
        cls._queries = {}
        cls._hydrators = {}

        base_path = cls.get_base_path("queries")
        for name, func in getmembers(cls):
            query = LazyQueryRegistry.get(cls.__name__, name)
            hydrator = LazyHydratorRegistry.get(cls.__name__, name)
            if hydrator:
                cls._hydrators[name] = hydrator

            filename = name
            if cls.is_operation(func):
                ignore = False
            elif (
                isfunction(func)
                and not any(hasattr(base, name) for base in cls.__bases__)
                and not name.startswith("_")
            ):
                ignore = True
                filename = f"{cls.generic_prefix}{filename}"
            else:
                continue

            auto_exec = is_auto_exec(func)
            path = base_path / f"{filename}.sql"

            try:
                cls._queries[name] = cls.QUERY_CLASS(
                    name, cls._load_sql(query, path)
                )
            except FileNotFoundError:
                if ignore:
                    continue
                if auto_exec and strict:
                    raise MissingSQL(
                        f"Could not find SQL for {cls.__name__}.{name}. "
                        f"Looked for file named: {path}"
                    )
            setattr(cls, name, cls._setup(func))

        for path in base_path.glob("*.sql"):
            if path.stem not in cls._queries and (
                cls.is_query_name(path.stem)
                or path.stem.startswith(cls.generic_prefix)
            ):
                cls._queries[path.stem] = cls.QUERY_CLASS(
                    path.stem, cls._load_sql("", path)
                )

        cls._loaded = True

    @classmethod
    def _load_sql(cls, query: Optional[str], path: Path):
        if not query:
            with open(path, "r") as f:
                query = f.read()
        return convert_sql_params(query, cls.POSITIONAL_SUB, cls.KEYWORD_SUB)

    @classmethod
    def is_operation(cls, obj):
        """Check if the object is a method that starts with a query prefix."""
        if isfunction(obj):
            return cls.is_query_name(obj.__name__)
        return False

    @classmethod
    def is_query_name(cls, name: str):
        return any(name.startswith(prefix) for prefix in cls.verb_prefixes)

    @staticmethod
    def _setup(func):
        """
        Responsible for executing a DB query and passing the result off to a
        hydrator.

        If the Executor does not contain any code, then the assumption is that
        we should automatically execute the in memory SQL, and passing the
        results off to the base Hydrator.
        """
        sig = signature(func)
        auto_exec = is_auto_exec(func)
        model = sig.return_annotation
        as_list = False
        allow_none = False
        name = func.__name__

        if model is not None and (origin := get_origin(model)):
            check_model = True
            if origin is UnionType or origin is Union:
                args = get_args(model)
                allow_none = True
                none_type = type(None)
                if len(args) == 2 and any(arg is none_type for arg in args):
                    model = args[0] if args[1] is none_type else args[1]
                    origin = get_origin(model)
                    if not origin:
                        check_model = False

            if check_model:
                as_list = bool(origin is list)
                as_dict = bool(origin is dict)
                if as_list:
                    model = get_args(model)[0]
                elif as_dict:
                    model = dict
                else:
                    raise MayimError(
                        f"{func} must return either a model or a list of "
                        "models. eg. -> Foo or List[Foo]"
                    )

        def decorator(f):
            @wraps(f)
            async def decorated_function(self: SQLExecutor, *args, **kwargs):
                if isinstance(self.pool, LazyPool):
                    raise MayimError(
                        "Connection pool to your database has not been setup. "
                    )
                self._context.set((model, name))
                if auto_exec:
                    query = self._queries[name]
                    bound = sig.bind(self, *args, **kwargs)
                    bound.apply_defaults()
                    params = {**bound.arguments}
                    params.pop("self", None)

                    if query.param_type is ParamType.KEYWORD:
                        results = await self._execute(
                            query.text,
                            model=model,
                            name=name,
                            as_list=as_list,
                            allow_none=allow_none,
                            params=params,
                        )
                    elif query.param_type is ParamType.POSITIONAL:
                        results = await self._execute(
                            query.text,
                            model=model,
                            as_list=as_list,
                            allow_none=allow_none,
                            posargs=list(params.values()),
                        )
                    else:
                        results = await self._execute(
                            query.text,
                            model=model,
                            as_list=as_list,
                            allow_none=allow_none,
                        )

                    if model is None:
                        return None
                else:
                    results = await f(self, *args, **kwargs)

                return results

            return decorated_function

        return decorator(func)
