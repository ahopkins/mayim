from __future__ import annotations

from functools import wraps
from inspect import getmembers, isfunction, signature
from pathlib import Path
from typing import Optional, Type, get_args, get_origin


from mayim.convert import convert_sql_params
from mayim.exception import MayimError
from mayim.registry import LazySQLRegistry

from .base import Executor, Query, is_auto_exec


class SQLExecutor(Executor):
    def execute(
        self,
        query: str,
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        **values,
    ):
        query = convert_sql_params(query)
        return self._execute(
            query=query, model=model, as_list=as_list, **values
        )

    async def _execute(
        self,
        query: str,
        model: Optional[Type[object]] = None,
        as_list: bool = False,
        **values,
    ):
        ...

    def run_sql(
        self,
        query: str = "",
        as_list: bool = False,
        **values,
    ):
        if query:
            query = convert_sql_params(query)
        else:
            _, query_name = self._context.get()
            # TODO:
            # - Fixed for positional
            query = (self._queries[query_name]).query
        return self._run_sql(query=query, as_list=as_list, **values)

    async def _run_sql(
        self,
        query: str,
        as_list: bool = False,
        **values,
    ):
        ...

    def _get_method(self, as_list: bool):
        ...

    @classmethod
    def _load(cls) -> None:
        cls._queries = {}

        base_path = cls.get_base_path("queries")
        for name, func in getmembers(cls):
            query = LazySQLRegistry.get(cls.__name__, name)

            if cls._isoperation(func):
                ignore = False
                filename = name
            elif (
                isfunction(func)
                and not any(hasattr(base, name) for base in cls.__bases__)
                and not name.startswith("_")
            ):
                ignore = True
                filename = f"mayim_{filename}"
            else:
                continue

            auto_exec = is_auto_exec(func)
            path = base_path / f"{filename}.sql"

            try:
                cls._queries[name] = Query(cls._load_sql(query, path), True)
            except FileNotFoundError:
                if auto_exec or ignore:
                    ...
            else:
                setattr(cls, name, cls._setup(func))
        cls._loaded = True

    @staticmethod
    def _isoperation(obj):
        """Check if the object is a method that starts with get_ or create_"""
        if isfunction(obj):
            return (
                obj.__name__.startswith("select_")
                or obj.__name__.startswith("insert_")
                or obj.__name__.startswith("update_")
                or obj.__name__.startswith("delete_")
            )
        return False

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
            if not as_list:
                return MayimError(
                    f"{func} must return either a model or a list of models. "
                    "eg. -> Foo or List[Foo]"
                )
            model = get_args(model)[0]

        def decorator(f):
            @wraps(f)
            async def decorated_function(self: SQLExecutor, *args, **kwargs):
                self._context.set((model, name))
                if auto_exec:
                    query = self._queries[name]
                    bound = sig.bind(self, *args, **kwargs)
                    bound.apply_defaults()
                    values = {**bound.arguments}
                    values.pop("self", None)

                    if query.named_args:
                        results = await self._execute(
                            query.query,
                            model=model,
                            as_list=as_list,
                            **values,
                        )
                    else:
                        raise Exception("POSITIONAL")
                        # results = await self._execute(
                        #     query.query,
                        #     model=model,
                        #     as_list=as_list,
                        #     *values.values(),
                        # )

                    if model is None:
                        return None
                else:
                    results = await f(self, *args, **kwargs)

                return results

            return decorated_function

        return decorator(func)
