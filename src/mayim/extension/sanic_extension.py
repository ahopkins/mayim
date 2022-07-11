from inspect import isclass
from typing import Optional, Sequence, Type, Union

from mayim import Executor, Hydrator, Mayim
from mayim.exception import MayimError
from mayim.extension.statistics import SQLCounterMixin
from mayim.interface.base import BaseInterface
from mayim.registry import InterfaceRegistry, Registry

from typing import DefaultDict
from collections import defaultdict

try:
    from sanic.log import logger
    from sanic_ext import Extend
    from sanic_ext.extensions.base import Extension
    from sanic import Request
    from sanic.helpers import _default, Default

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

        if self._display_counters():
            self._init_counters()

    def render_label(self):
        length = len(Registry())
        s = "" if length == 1 else "s"
        return f"[{length} executor{s}]"

    def _display_counters(self) -> bool:
        if isinstance(self.counters, bool):
            return self.counters

        def _is_sql_counter(executor):
            executor_class = (
                executor if isclass(executor) else executor.__class__
            )
            return SQLCounterMixin in executor_class.mro()

        return any(_is_sql_counter(executor) for executor in self.executors)

    def _init_counters(self):
        @self.app.on_request
        async def setup_qry_counter(request: Request):
            registry = Registry()
            for executor in registry.values():
                if hasattr(executor, "reset"):
                    executor.reset()

        @self.app.on_response
        async def setup_qry_display(request: Request, _):
            COLUMN_SIZE = 6
            registry = Registry()
            statistics = [
                dict(executor._counter)
                for executor in registry.values()
                if hasattr(executor, "_counter")
            ]
            keys = list(
                sorted(
                    {
                        key.rjust(COLUMN_SIZE)
                        for executor in registry.values()
                        if hasattr(executor, "_counter")
                        for key in executor._counter.keys()
                    }
                )
            )
            max_executor_name = max(map(len, registry.keys()))
            headers = " | ".join([" " * max_executor_name, *keys])
            row_data = [
                " | ".join(
                    [
                        name.rjust(max_executor_name),
                        *[
                            str(executor._counter.get(key, "-")).rjust(
                                COLUMN_SIZE
                            )
                            for key in keys
                        ],
                    ]
                )
                for name, executor in sorted(
                    registry.items(), key=lambda x: x[0]
                )
                if hasattr(executor, "_counter")
            ]
            if not row_data:
                row_data = ["No executor counters found"]
            rows = "\n".join(row_data)
            total_values: DefaultDict[str, int] = defaultdict(int)
            for stats in statistics:
                for key, value in stats.items():
                    total_values[key] += value
            divider = "=" * len(row_data[0])
            totals = " | ".join(
                [
                    "TOTALS".rjust(max_executor_name),
                    *[
                        str(total_values.get(key, "-")).rjust(COLUMN_SIZE)
                        for key in keys
                    ],
                ]
            )
            title = "QUERY COUNTERS".center(len(divider))

            logger.info(
                f"SQL Statistics Report\n\n{title}\n\n{headers}\n"
                f"{rows}\n{divider}\n{totals}\n\n"
            )
