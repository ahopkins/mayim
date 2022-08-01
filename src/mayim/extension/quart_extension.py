from __future__ import annotations

from typing import Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.exception import MayimError
<<<<<<< Updated upstream
=======
from mayim.extension.statistics import (
    display_statistics,
    SQLStatisticsMiddleware,
)
>>>>>>> Stashed changes
from mayim.interface.base import BaseInterface
from mayim.registry import InterfaceRegistry, Registry

try:
    from quart import Quart

    QUART_INSTALLED = True
except ModuleNotFoundError:
    QUART_INSTALLED = False
    Quart = type("Quart", (), {})  # type: ignore


<<<<<<< Updated upstream
=======
class Default:
    ...


_default = Default()


>>>>>>> Stashed changes
class QuartMayimExtension:
    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        app: Optional[Quart] = None,
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
<<<<<<< Updated upstream
=======

        if display_statistics(self.counters, self.executors):
            app.asgi_app = SQLStatisticsMiddleware(  # type: ignore
                app.asgi_app, logger
            )
>>>>>>> Stashed changes
