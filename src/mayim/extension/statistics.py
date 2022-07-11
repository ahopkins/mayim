from collections import defaultdict
from inspect import isclass
from typing import DefaultDict

from mayim.executor.sql import SQLExecutor
from mayim.registry import Registry


class SQLCounterMixin(SQLExecutor):
    def __init__(self, *args, **kwargs) -> None:
        self.reset()
        super().__init__(*args, **kwargs)

    def reset(self):
        self._counter = defaultdict(int)

    def _run_sql(self, *args, name: str = "", **kwargs):
        if name and self.is_query_name(name):
            query_type, _ = name.split("_", 1)
        else:
            query_type = "unknown"
        self._counter[query_type] += 1
        kwargs["name"] = name
        return super()._run_sql(*args, **kwargs)


def display_counters(counters, executors) -> bool:
    if isinstance(counters, bool):
        return counters

    def _is_sql_counter(executor):
        executor_class = executor if isclass(executor) else executor.__class__
        return SQLCounterMixin in executor_class.mro()

    return any(_is_sql_counter(executor) for executor in executors)


def setup_qry_counter(*_):
    registry = Registry()
    for executor in registry.values():
        if hasattr(executor, "reset"):
            executor.reset()


def setup_qry_display(logger, *_):
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
                    str(executor._counter.get(key, "-")).rjust(COLUMN_SIZE)
                    for key in keys
                ],
            ]
        )
        for name, executor in sorted(registry.items(), key=lambda x: x[0])
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
