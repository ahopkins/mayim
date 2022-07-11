from collections import defaultdict

from mayim.executor.sql import SQLExecutor


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
