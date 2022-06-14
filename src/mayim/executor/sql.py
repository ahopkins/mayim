from __future__ import annotations

from functools import wraps
from inspect import getmembers, isawaitable, isfunction, signature
from pathlib import Path
from typing import Any, Dict, Optional, Sequence, Type, get_args, get_origin

from mayim.convert import convert_sql_params
from mayim.exception import MayimError, RecordNotFound
from mayim.query.sql import ParamType, SQLQuery
from mayim.registry import LazyHydratorRegistry, LazySQLRegistry

from .base import Executor, is_auto_exec


class SQLExecutor(Executor[SQLQuery]):
    ENABLED: bool = False
    QUERY_CLASS = SQLQuery
    _filename_prefix: str = "mayim_"

    def execute(
        self,
        query: str,
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        query = convert_sql_params(query)
        return self._execute(
            query=query,
            name=name,
            model=model,
            as_list=as_list,
            posargs=posargs,
            params=params,
        )

    async def _execute(
        self,
        query: str,
        name: str = "",
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        posargs: Optional[Sequence[Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        no_result = False
        if model is None:
            model, _ = self._context.get()
        if model is None:
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
            raise RecordNotFound("not found")
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
            query = convert_sql_params(query)
        else:
            _, query_name = self._context.get()
            # TODO:
            # - Fixed for positional
            query = (self._queries[query_name]).text
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

    def _get_method(self, as_list: bool):
        ...

    @classmethod
    def _load(cls) -> None:
        cls._queries = {}
        cls._hydrators = {}

        base_path = cls.get_base_path("queries")
        for name, func in getmembers(cls):
            query = LazySQLRegistry.get(cls.__name__, name)
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
                filename = f"{cls._filename_prefix}{filename}"
            else:
                continue

            auto_exec = is_auto_exec(func)
            path = base_path / f"{filename}.sql"

            try:
                cls._queries[name] = cls.QUERY_CLASS(
                    name, cls._load_sql(query, path)
                )
            except FileNotFoundError:
                if auto_exec or ignore:
                    ...
            else:
                setattr(cls, name, cls._setup(func))
        cls._loaded = True

    @classmethod
    def is_operation(cls, obj):
        """Check if the object is a method that starts with get_ or create_"""
        if isfunction(obj):
            return cls.is_query_name(obj.__name__)
        return False

    @staticmethod
    def is_query_name(name: str):
        return (
            name.startswith("select_")
            or name.startswith("insert_")
            or name.startswith("update_")
            or name.startswith("delete_")
        )

    @staticmethod
    def _load_sql(query: Optional[str], path: Path):
        if not query:
            with open(path, "r") as f:
                query = f.read()
        return convert_sql_params(query)

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
        name = func.__name__

        if model is not None and (origin := get_origin(model)):
            as_list = bool(origin is list)
            as_dict = bool(origin is dict)
            if as_list:
                model = get_args(model)[0]
            elif as_dict:
                model = dict
            else:
                raise MayimError(
                    f"{func} must return either a model or a list of models. "
                    "eg. -> Foo or List[Foo]"
                )

        def decorator(f):
            @wraps(f)
            async def decorated_function(self: SQLExecutor, *args, **kwargs):
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
                            params=params,
                        )
                    elif query.param_type is ParamType.POSITIONAL:
                        results = await self._execute(
                            query.text,
                            model=model,
                            as_list=as_list,
                            posargs=list(params.values()),
                        )
                    else:
                        results = await self._execute(
                            query.text,
                            model=model,
                            as_list=as_list,
                        )

                    if model is None:
                        return None
                else:
                    results = await f(self, *args, **kwargs)

                return results

            return decorated_function

        return decorator(func)
