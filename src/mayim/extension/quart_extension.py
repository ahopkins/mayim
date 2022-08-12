from __future__ import annotations

from logging import getLogger
from typing import Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.base.interface import BaseInterface
from mayim.exception import MayimError
from mayim.extension.statistics import (
    SQLStatisticsMiddleware,
    display_statistics,
)
from mayim.registry import InterfaceRegistry, Registry

logger = getLogger("quart.app")
try:
    from quart import Quart

    QUART_INSTALLED = True
except ModuleNotFoundError:
    QUART_INSTALLED = False
    Quart = type("Quart", (), {})  # type: ignore


class Default:
    ...


_default = Default()


class QuartMayimExtension:
    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        app: Optional[Quart] = None,
        counters: Union[Default, bool] = _default,
    ):
        if not QUART_INSTALLED:
            raise MayimError(
                "Could not locate Quart. It must be installed to use "
                "QuartMayimExtension. Try: pip install quart"
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

    def init_app(self, app: Quart) -> None:
        @app.while_serving
        async def lifespan():
            Mayim(executors=self.executors, **self.mayim_kwargs)
            for interface in InterfaceRegistry():
                await interface.open()

            yield

            for interface in InterfaceRegistry():
                await interface.close()

        if display_statistics(self.counters, self.executors):
            app.asgi_app = SQLStatisticsMiddleware(  # type: ignore
                app.asgi_app, logger
            )
