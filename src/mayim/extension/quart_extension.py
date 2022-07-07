from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.interface.base import BaseInterface
from mayim.registry import InterfaceRegistry, Registry


if TYPE_CHECKING:
    from quart import Quart


class QuartMayimExtension:
    name = "mayim"

    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ):
        self.executors = executors or []
        for executor in self.executors:
            Registry().register(executor)
        self.mayim_kwargs = {
            "dsn": dsn,
            "hydrator": hydrator,
            "pool": pool,
        }

    def init_app(self, app: Quart) -> None:
        @app.while_serving
        async def lifespan():
            Mayim(executors=self.executors, **self.mayim_kwargs)
            for interface in InterfaceRegistry():
                await interface.open()

            yield

            for interface in InterfaceRegistry():
                await interface.close()
