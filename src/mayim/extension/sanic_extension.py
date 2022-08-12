from typing import Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.base.interface import BaseInterface
from mayim.exception import MayimError
from mayim.extension.statistics import (
    display_statistics,
    log_statistics_report,
    setup_query_counter,
)
from mayim.registry import InterfaceRegistry, Registry

try:
    from sanic.helpers import Default, _default
    from sanic.log import logger
    from sanic.signals import Event
    from sanic_ext import Extend
    from sanic_ext.extensions.base import Extension

    SANIC_INSTALLED = True
except ModuleNotFoundError:
    SANIC_INSTALLED = False
    Extension = type("Extension", (), {})  # type: ignore
    Extend = type("Extend", (), {})  # type: ignore


class SanicMayimExtension(Extension):
    name = "mayim"

    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
        counters: Union[Default, bool] = _default,
    ):
        if not SANIC_INSTALLED:
            raise MayimError(
                "Could not locate either Sanic or Sanic Extensions. "
                "Both libraries must be installed to use SanicMayimExtension. "
                "Try: pip install sanic[ext]"
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

    def startup(self, bootstrap: Extend) -> None:
        for executor in Registry().values():
            if isinstance(executor, Executor):
                bootstrap.dependency(executor)
            else:
                bootstrap.add_dependency(
                    executor, lambda *_: Mayim.get(executor)
                )

        @self.app.before_server_start
        async def setup(_):
            Mayim(executors=self.executors, **self.mayim_kwargs)
            for interface in InterfaceRegistry():
                logger.info(f"Opening {interface}")
                await interface.open()

        @self.app.after_server_stop
        async def shutdown(_):
            for interface in InterfaceRegistry():
                logger.info(f"Closing {interface}")
                await interface.close()

        for executor in Registry().values():
            if isinstance(executor, Executor):
                bootstrap.dependency(executor)
            else:
                bootstrap.add_dependency(
                    executor, lambda *_: Mayim.get(executor)
                )
        if display_statistics(self.counters, self.executors):
            self.app.signal(Event.HTTP_LIFECYCLE_REQUEST)(setup_query_counter)

        if display_statistics(self.counters, self.executors):
            self.app.on_request(setup_query_counter)

            @self.app.on_response
            async def display(*_):
                log_statistics_report(logger)

    def render_label(self):
        length = len(Registry())
        s = "" if length == 1 else "s"
        return f"[{length} executor{s}]"
