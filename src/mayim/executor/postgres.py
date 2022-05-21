from __future__ import annotations

from functools import wraps
from inspect import isawaitable, signature
from typing import Optional, Type, get_args, get_origin

from psycopg.rows import dict_row

from mayim.convert import convert_sql_params
from mayim.exception import MayimError

from .base import Executor, is_auto_exec


class PostgresExecutor(Executor):
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
        if model is None:
            model, _ = self._context.get()
        factory = self.hydrator._make(model)
        raw = await self._run_sql(query=query, as_list=as_list, **values)
        results = factory(raw)
        if isawaitable(results):
            results = await results
        return results

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
            query = self._queries[query_name]
        return self._run_sql(query=query, as_list=as_list, **values)

    async def _run_sql(
        self,
        query: str,
        as_list: bool = False,
        **values,
    ):
        method_name = self._get_method(as_list=as_list)
        async with self.pool.connection() as conn:
            cursor = await conn.execute(query, values)
            cursor.row_factory = dict_row
            raw = await getattr(cursor, method_name)()
            return raw

    def _get_method(self, as_list: bool):
        return "fetchall" if as_list else "fetchone"

    @staticmethod
    def setup(func):
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
            async def decorated_function(
                self: PostgresExecutor, *args, **kwargs
            ):
                if auto_exec:
                    query = self._queries[name]
                    bound = sig.bind(self, *args, **kwargs)
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
                    results = await f(self, *args, **kwargs)

                return results

            return decorated_function

        return decorator(func)
