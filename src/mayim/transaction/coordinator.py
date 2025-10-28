from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, List, Optional, Union, cast
from uuid import uuid4

from mayim.exception import MayimError
from mayim.registry import Registry

from .connection_manager import TransactionConnectionManager
from .interfaces import (
    IsolationLevel,
    SavepointNotSupportedError,
    TransactionError,
)
from .savepoint import Savepoint

if TYPE_CHECKING:
    from mayim.sql.executor import SQLExecutor

logger = logging.getLogger(__name__)


class TransactionCoordinator:
    def __init__(
        self,
        executors: List[Union[type, SQLExecutor]],
        use_2pc: bool = False,
        timeout: Optional[float] = None,
        isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ):
        self.transaction_id = f"txn_{uuid4().hex[:8]}"
        self._isolation_level = isolation_level
        self.timeout = timeout or 300.0  # 5 minutes default
        self.use_2pc = use_2pc
        self._executors = self._resolve_executors(executors)
        self._connection_manager = TransactionConnectionManager(
            self.transaction_id, self.timeout
        )
        self._begun = False
        self._committed = False
        self._rolled_back = False
        self._start_time = 0.0
        self._savepoints: dict[str, Savepoint] = {}

        logger.debug(
            "Transaction %s created with %d executors",
            self.transaction_id,
            len(self._executors),
        )

    def _resolve_executors(
        self, executors: List[Union[type, SQLExecutor]]
    ) -> List[SQLExecutor]:
        """Resolve executor classes to instances"""
        resolved: List[SQLExecutor] = []
        registry = Registry()
        for executor in executors:
            resolved_executor: Union[type, SQLExecutor] = executor
            if isinstance(executor, type):
                resolved_executor = cast(
                    SQLExecutor, registry.get(executor.__name__)
                )
            assert not isinstance(
                resolved_executor, type
            ), f"Executor {executor} could not be resolved to an instance"
            resolved.append(resolved_executor)
        return resolved

    async def begin(self) -> None:
        """Begin the transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError(
                f"Transaction {self.transaction_id} already finalized"
            )

        if self._begun:
            raise TransactionError(
                f"Transaction {self.transaction_id} already begun"
            )

        logger.debug("Beginning transaction %s", self.transaction_id)

        try:
            # Start transaction on all connections
            begin_sql = f"BEGIN ISOLATION LEVEL {self._isolation_level.value}"
            await self._execute_on_all_pools(begin_sql)

            # Inject connections into executors
            for executor in self._executors:
                self._connection_manager.inject_into_executor(executor)

            self._begun = True
            self._start_time = time.time()
            logger.info(
                "Transaction %s started successfully", self.transaction_id
            )

        except Exception as e:
            await self._cleanup()
            raise TransactionError(
                f"Failed to begin transaction {self.transaction_id}: {e}"
            ) from e

    async def commit(self) -> None:
        """Commit the transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError(
                f"Transaction {self.transaction_id} already finalized"
            )

        if not self._begun:
            raise TransactionError(
                f"Transaction {self.transaction_id} not begun"
            )

        logger.debug("Committing transaction %s", self.transaction_id)

        if self._start_time and time.time() - self._start_time > self.timeout:
            logger.warning(
                "Transaction %s timed out, rolling back", self.transaction_id
            )
            try:
                await self._guaranteed_rollback()
            except Exception as rollback_error:
                logger.critical(
                    "Rollback after timeout also failed: %s", rollback_error
                )
            raise MayimError(
                f"Transaction timed out after {self.timeout} seconds"
            )

        try:
            await self._connection_manager.execute_on_all("COMMIT")
            self._committed = True
            self._begun = False

            logger.info(
                "Transaction %s committed successfully", self.transaction_id
            )

        except Exception as e:
            logger.error(
                "Commit failed for %s, attempting rollback: %s",
                self.transaction_id,
                e,
            )
            try:
                await self._guaranteed_rollback()
            except Exception as rollback_error:
                logger.critical(
                    "Rollback after failed commit also failed: %s",
                    rollback_error,
                )

            raise TransactionError(
                f"Failed to commit transaction {self.transaction_id}: {e}"
            ) from e
        finally:
            await self._cleanup()

    async def rollback(self) -> None:
        """Rollback the transaction"""
        if self._committed or self._rolled_back:
            raise TransactionError(
                f"Transaction {self.transaction_id} already finalized"
            )

        if not self._begun:
            raise TransactionError(
                f"Transaction {self.transaction_id} not begun"
            )

        logger.debug("Rolling back transaction %s", self.transaction_id)

        try:
            await self._guaranteed_rollback()
            logger.info(
                "Transaction %s rolled back successfully", self.transaction_id
            )
        except Exception as e:
            logger.critical(
                "CRITICAL: Rollback failed for %s: %s", self.transaction_id, e
            )
            # Mark as rolled back even if SQL failed to prevent
            # further operations
            self._rolled_back = True
            raise TransactionError(
                f"Failed to rollback transaction {self.transaction_id}: {e}"
            ) from e
        finally:
            await self._cleanup()

    async def _guaranteed_rollback(self) -> None:
        """GUARANTEED rollback execution - this ALWAYS executes SQL
        ROLLBACK commands"""
        logger.debug(
            "Executing GUARANTEED rollback for %s", self.transaction_id
        )
        await self._connection_manager.execute_on_all("ROLLBACK")
        self._rolled_back = True
        self._begun = False

        logger.debug(
            "SQL ROLLBACK executed on all connections for %s",
            self.transaction_id,
        )

    async def _execute_on_all_pools(self, sql_command: str) -> None:
        """Execute SQL command by getting connections for all pools first"""
        for executor in self._executors:
            await self._connection_manager.get_connection(executor.pool)
        await self._connection_manager.execute_on_all(sql_command)

    async def _cleanup(self) -> None:
        """Clean up resources"""
        try:
            await self._connection_manager.cleanup()

            for executor in self._executors:
                executor.pool._clear_transaction_connection()

        except Exception as e:
            logger.error(
                "Error during cleanup of transaction %s: %s",
                self.transaction_id,
                e,
            )

    async def __aenter__(self):
        """Async context manager entry"""
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        try:
            # Only commit/rollback if transaction hasn't been finalized yet
            if not self._committed and not self._rolled_back:
                if exc_type is None:
                    await self.commit()
                else:
                    await self.rollback()
        except Exception as e:
            logger.error(
                "Error in context manager exit for %s: %s",
                self.transaction_id,
                e,
            )
            # Re-raise the original exception if there was one,
            # otherwise raise the new one
            if exc_type is None:
                raise

        # Return False to propagate any original exception
        return False

    @property
    def is_active(self) -> bool:
        """Check if transaction is active"""
        return self._begun and not self._committed and not self._rolled_back

    @property
    def is_committed(self) -> bool:
        """Check if transaction is committed"""
        return self._committed

    @property
    def is_rolled_back(self) -> bool:
        """Check if transaction is rolled back"""
        return self._rolled_back

    @property
    def executors(self) -> List[SQLExecutor]:
        """Get the list of executors involved in this transaction"""
        return self._executors

    @property
    def isolation_level(self) -> str:
        """Get the isolation level as a string"""
        return self._isolation_level.value

    async def savepoint(self, name: str) -> Savepoint:
        """Create a savepoint for nested rollback points"""
        if not self._begun:
            raise TransactionError(
                f"Transaction {self.transaction_id} not begun"
            )

        if self._committed or self._rolled_back:
            raise TransactionError(
                f"Transaction {self.transaction_id} already finalized"
            )

        if name in self._savepoints:
            raise TransactionError(f"Savepoint {name} already exists")

        await self._check_savepoint_support()

        logger.debug(
            "Creating savepoint %s in transaction %s",
            name,
            self.transaction_id,
        )

        try:
            await self._connection_manager.execute_on_all(f"SAVEPOINT {name}")
            savepoint = Savepoint(name, self)
            self._savepoints[name] = savepoint

            logger.info(
                "Created savepoint %s in transaction %s",
                name,
                self.transaction_id,
            )
            return savepoint

        except Exception as e:
            logger.error("Failed to create savepoint %s: %s", name, e)
            raise TransactionError(
                f"Failed to create savepoint {name}: {e}"
            ) from e

    async def _check_savepoint_support(self) -> None:
        """Check if all executors support savepoints (PostgreSQL/MySQL only)"""
        for executor in self._executors:
            db_type = getattr(
                executor.pool, "db_type", None
            ) or self._detect_db_type(executor)
            if db_type not in ("postgresql", "mysql"):
                raise SavepointNotSupportedError(
                    f"Savepoints not supported for database type: {db_type}. "
                    "Only PostgreSQL and MySQL are supported."
                )

    def _detect_db_type(self, executor) -> str:
        """Detect database type from pool class and scheme"""
        pool = executor.pool
        postgres = "postgresql"
        mysql = "mysql"
        sqlite = "sqlite"

        # Check for scheme attribute first (most reliable)
        if hasattr(pool, "scheme"):
            scheme = pool.scheme.lower()
            if scheme.startswith("postgres"):
                return postgres
            elif scheme == "mysql":
                return mysql
            elif scheme == "sqlite":
                return sqlite

        # Check class name for Pool types
        class_name = pool.__class__.__name__.lower()
        if "postgres" in class_name or "pg" in class_name:
            return postgres
        elif "mysql" in class_name:
            return mysql
        elif "sqlite" in class_name:
            return sqlite

        # Fallback to module name detection
        pool_module = pool.__class__.__module__.lower()
        if "psycopg" in pool_module or "asyncpg" in pool_module:
            return postgres
        elif "aiomysql" in pool_module or "mysql" in pool_module:
            return mysql
        elif "sqlite" in pool_module or "aiosqlite" in pool_module:
            return sqlite

        return "unknown"
