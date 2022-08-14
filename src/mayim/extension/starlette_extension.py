from __future__ import annotations

from logging import INFO, Logger, basicConfig, getLogger
from typing import Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.base.interface import BaseInterface
from mayim.exception import MayimError
from mayim.extension.statistics import (
    SQLStatisticsMiddleware,
    display_statistics,
)
from mayim.registry import InterfaceRegistry, Registry

try:
    from starlette.applications import Starlette

    STARLETTE_INSTALLED = True
except ModuleNotFoundError:
    STARLETTE_INSTALLED = False
    Starlette = type("Starlette", (), {})  # type: ignore


class Default:
    ...


_default = Default()


class StarletteMayimExtension:
    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        app: Optional[Starlette] = None,
        counters: Union[Default, bool] = _default,
    ):
        if not STARLETTE_INSTALLED:
            raise MayimError(
                "Could not locate Starlette. It must be installed to use "
                "StarletteMayimExtension. Try: pip install starlette"
            )
        self.executors = executors or []
        for executor in self.executors:
            Registry().register(executor)
        self.mayim_kwargs = {
            "dsn": dsn,
            "hydrator": hydrator,
            "pool": pool,
        }
        self.counters = counters
        if app is not None:
            self.init_app(app)

    def init_app(
        self, app: Starlette, logger: Optional[Logger] = None
    ) -> None:
        async def startup():
            Mayim(executors=self.executors, **self.mayim_kwargs)
            for interface in InterfaceRegistry():
                await interface.open()

        async def shutdown():
            for interface in InterfaceRegistry():
                await interface.close()

        app.add_event_handler("startup", startup)
        app.add_event_handler("shutdown", shutdown)

        if display_statistics(self.counters, self.executors):
            if logger is None:
                basicConfig(level=INFO)
                logger = getLogger("mayim")
            app.add_middleware(SQLStatisticsMiddleware, logger=logger)
