from importlib.metadata import version

from .base.executor import Executor
from .base.hydrator import Hydrator
from .decorator import hydrator, query, register
from .mayim import Mayim
from .sql.mysql.executor import MysqlExecutor
from .sql.mysql.interface import MysqlPool
from .sql.postgres.executor import PostgresExecutor
from .sql.postgres.interface import PostgresPool
from .sql.sqlite.executor import SQLiteExecutor
from .sql.sqlite.interface import SQLitePool

__version__ = version("mayim")

__all__ = (
    "hydrator",
    "register",
    "query",
    "Executor",
    "Hydrator",
    "Mayim",
    "MysqlExecutor",
    "MysqlPool",
    "PostgresExecutor",
    "SQLiteExecutor",
    "PostgresPool",
    "SQLitePool",
)
