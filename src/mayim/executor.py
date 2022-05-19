from __future__ import annotations
from contextvars import ContextVar

from inspect import (
    getmembers,
    getmodule,
    getsourcelines,
    isawaitable,
    isclass,
    isfunction,
)
from pathlib import Path
import re
from typing import Dict, Optional, Tuple, Type, TypeVar, Union

from mayim.exception import MayimError
from mayim.interface.base import BaseInterface

from .decorator import execute
from .hydrator import Hydrator
from psycopg.rows import dict_row

CONVERT_PATTERN = re.compile(r"(\$([a-z0-9_]+))")


class Registry(dict):
    _singleton = None

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls.reset()
        return cls._singleton

    def register(self, executor: Executor) -> None:
        if executor.__class__ not in self:
            self[executor.__class__.__name__] = executor

    @classmethod
    def reset(cls):
        cls._singleton = super().__new__(cls)  # type: ignore


class Executor:
    """
    Responsible for being the interface between the DB and the application
    """

    _queries: Dict[str, str]
    _fallback_hydrator: Hydrator
    _fallback_pool: Optional[BaseInterface]

    def __init__(
        self,
        pool: Optional[BaseInterface] = None,
        hydrator: Optional[Hydrator] = None,
    ) -> None:
        pool = pool or getattr(self.__class__, "_fallback_pool", None)
        if not pool:
            raise MayimError(
                f"Cannot instantiate {self.__class__} without a pool"
            )
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
        query = CONVERT_PATTERN.sub(r"%(\2)s", query, 0)
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
            query = CONVERT_PATTERN.sub(r"%(\2)s", query, 0)
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
