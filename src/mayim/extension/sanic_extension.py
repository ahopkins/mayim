from typing import Optional, Sequence, Type, Union

from sanic_ext import Extend
from sanic_ext.extensions.base import Extension

from mayim import Executor, Hydrator, Mayim
from mayim.interface.base import BaseInterface
from mayim.registry import Registry


class SanicMayimExtension(Extension):
    name = "mayim"

    def __init__(
        self,
        *,
        executors: Optional[Sequence[Union[Type[Executor], Executor]]] = None,
        dsn: str = "",
        hydrator: Optional[Hydrator] = None,
        pool: Optional[BaseInterface] = None,
    ):
        # raise NotImplementedError(
        #     "This is a placeholder feature and will not be released until "
        #     "sometime after June 2022."
        # )
        self.executors = executors or []
        for executor in self.executors:
            Registry().register(executor)
        self.mayim_kwargs = {
            "dsn": dsn,
            "hydrator": hydrator,
            "pool": pool,
        }

    def startup(self, bootstrap: Extend) -> None:
        @self.app.before_server_start
        async def setup(app):
            Mayim(executors=self.executors, **self.mayim_kwargs)

        for executor in Registry().values():
            if isinstance(executor, Executor):
                bootstrap.dependency(executor)
            else:
                bootstrap.add_dependency(
                    executor, lambda *_: Mayim.get(executor)
                )

    def render_label(self):
        length = len(Registry())
        s = "" if length == 1 else "s"
        return f"[{length} executor{s}]"
