"""
Transaction coordination module for Mayim.
Provides global transaction management across multiple executors.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from enum import Enum
from inspect import isclass
from time import time
from typing import Any, Dict, List, Optional, Set, Type, Union

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError

# Global transaction context that all executors can access
_global_transaction: ContextVar[Optional["GlobalTransactionContext"]] = (
    ContextVar("global_transaction", default=None)
)


class TransactionState(Enum):
    """Transaction state machine states"""

    PENDING = "pending"  # Created but not started
    ACTIVE = "active"  # Transaction has begun
    COMMITTED = "committed"  # Transaction committed successfully
    ROLLED_BACK = "rolled_back"  # Transaction was rolled back


class GlobalTransactionContext:
    """
    Manages a global transaction context across multiple executors.
    Ensures all executors sharing the same pool use the same connection.
    """

    def __init__(self):
        # Map from pool instance to acquired connection
        self.connections: Dict[BaseInterface, Any] = {}
        # Map from pool instance to executors using it
        self.pool_executors: Dict[BaseInterface, Set[Any]] = {}
        # Track which pools should commit
        self.commit_flags: Dict[BaseInterface, bool] = {}
        # Stack for cleanup
        self.stack = AsyncExitStack()
        # Transaction state
        self.state = TransactionState.PENDING
        # Two-phase commit support
        self.prepared_pools: Set[BaseInterface] = set()
        self.supports_2pc = False

    async def add_executor(self, executor):
        """Register an executor in this transaction"""
        from mayim.sql.executor import SQLExecutor

        if not isinstance(executor, SQLExecutor):
            raise MayimError(
                f"Only SQL executors can participate in transactions, got {type(executor)}"
            )

        pool = executor.pool

        # Track this executor
        if pool not in self.pool_executors:
            self.pool_executors[pool] = set()
        self.pool_executors[pool].add(executor)

        # If we haven't acquired a connection for this pool yet, do so
        if pool not in self.connections:
            # Acquire a connection and keep it for all executors using this pool
            conn = await self.stack.enter_async_context(pool.connection())
            self.connections[pool] = conn
            # Set the connection in the pool's context var so all executors see it
            pool._connection.set(conn)
            pool._transaction.set(True)
            self.commit_flags[pool] = True  # Default to commit

    def get_connection(self, pool: BaseInterface) -> Any:
        """Get the connection for a given pool"""
        return self.connections.get(pool)

    async def prepare_all(self) -> bool:
        """Prepare all connections for commit (2PC phase 1)"""
        self.prepared_pools.clear()

        for pool, conn in self.connections.items():
            try:
                # Check if connection supports prepare
                if hasattr(conn, "prepare"):
                    await conn.prepare()
                    self.prepared_pools.add(pool)
                else:
                    # If any connection doesn't support 2PC, we can't use it
                    return False
            except Exception as e:
                # Prepare failed, rollback what we can
                await self.rollback_all()
                raise MayimError(f"Transaction prepare failed: {e}") from e

        return len(self.prepared_pools) == len(self.connections)

    async def rollback_all(self):
        """Rollback all connections"""
        for pool, conn in self.connections.items():
            pool._commit.set(False)
            try:
                # Check if connection has rollback method
                if hasattr(conn, "rollback"):
                    await conn.rollback()
            except Exception:
                pass  # Best effort rollback

    async def commit_all(self, prepared: bool = False):
        """Commit all connections that should be committed

        Args:
            prepared: Whether connections have already been prepared (2PC phase 2)
        """
        for pool, conn in self.connections.items():
            if self.commit_flags.get(pool, True):
                try:
                    # If we're in 2PC and this pool was prepared
                    if prepared and pool in self.prepared_pools:
                        # Use prepared commit if available
                        if hasattr(conn, "commit_prepared"):
                            await conn.commit_prepared()
                        else:
                            await conn.commit()
                    else:
                        # Regular commit
                        if hasattr(conn, "commit"):
                            await conn.commit()
                except Exception as e:
                    # If any commit fails, rollback all
                    await self.rollback_all()
                    raise MayimError(f"Transaction commit failed: {e}") from e

    async def cleanup(self):
        """Clean up all connections and state"""
        for pool in self.connections:
            pool._connection.set(None)
            pool._transaction.set(False)
            pool._commit.set(True)


class TransactionCoordinator:
    """
    Coordinates transactions across multiple executors.
    Supports both context manager and explicit transaction control.
    """

    def __init__(
        self,
        executors: List[Union[Type, Any]],
        use_2pc: bool = False,
        timeout: Optional[float] = None,
    ):
        """
        Initialize transaction coordinator.

        Args:
            executors: List of executor classes or instances to include in transaction
            use_2pc: Whether to use two-phase commit protocol if available
            timeout: Maximum duration in seconds before transaction is automatically rolled back
        """
        self._executors = executors
        self._context: Optional[GlobalTransactionContext] = None
        self._token = None
        self._state = TransactionState.PENDING
        self._stack: Optional[AsyncExitStack] = None
        self._use_2pc = use_2pc
        self._prepared = False
        self._timeout = timeout
        self._start_time: Optional[float] = None
        self._timeout_task: Optional[asyncio.Task] = None

    @property
    def is_active(self) -> bool:
        """Check if transaction is currently active"""
        return self._state == TransactionState.ACTIVE

    @property
    def is_committed(self) -> bool:
        """Check if transaction has been committed"""
        return self._state == TransactionState.COMMITTED

    @property
    def is_rolled_back(self) -> bool:
        """Check if transaction has been rolled back"""
        return self._state == TransactionState.ROLLED_BACK

    @property
    def executors(self) -> List:
        """Get list of executors in this transaction"""
        return self._executors

    async def begin(self):
        """
        Begin the transaction explicitly.
        Sets up connections and marks transaction as active.
        """
        if self._state == TransactionState.ACTIVE:
            raise MayimError("Transaction already active")
        if self._state in (
            TransactionState.COMMITTED,
            TransactionState.ROLLED_BACK,
        ):
            raise MayimError("Transaction already completed")

        # Create global transaction context
        self._context = GlobalTransactionContext()
        self._token = _global_transaction.set(self._context)
        self._stack = AsyncExitStack()

        try:
            await self._stack.__aenter__()
            self._context.stack = self._stack

            # Register all executors and acquire connections
            for executor in self._executors:
                await self._context.add_executor(executor)

            self._state = TransactionState.ACTIVE
            self._context.state = TransactionState.ACTIVE

            # Start timeout tracking
            self._start_time = time()
            if self._timeout is not None:
                self._timeout_task = asyncio.create_task(
                    self._timeout_monitor()
                )

        except Exception:
            # Clean up on failure
            await self._cleanup()
            raise

    async def prepare_all(self) -> bool:
        """
        Prepare all connections for commit (2PC phase 1).

        Returns:
            True if all connections were prepared successfully
        """
        if self._state != TransactionState.ACTIVE:
            raise MayimError("Transaction must be active to prepare")

        if self._context:
            self._prepared = await self._context.prepare_all()
            return self._prepared
        return False

    async def commit(self):
        """
        Commit the transaction.
        Commits all connections and marks transaction as committed.
        """
        if self._state == TransactionState.PENDING:
            raise MayimError("Transaction not active")

        # Check for timeout first
        if (
            self._check_timeout()
            or self._state == TransactionState.ROLLED_BACK
        ):
            if self._state != TransactionState.ROLLED_BACK:
                await self.rollback()
            raise MayimError("Transaction timed out")

        if self._state in (
            TransactionState.COMMITTED,
            TransactionState.ROLLED_BACK,
        ):
            raise MayimError("Transaction already completed")
        if self._state != TransactionState.ACTIVE:
            raise MayimError("Transaction not active")

        try:
            # If we're using 2PC and haven't prepared yet, do it now
            if self._use_2pc and not self._prepared:
                await self.prepare_all()

            # Commit all connections
            await self._context.commit_all(prepared=self._prepared)
            self._state = TransactionState.COMMITTED
            self._context.state = TransactionState.COMMITTED
        finally:
            # Clean up
            await self._cleanup()

    async def rollback(self):
        """
        Rollback the transaction.
        Rolls back all connections and marks transaction as rolled back.
        """
        if self._state == TransactionState.PENDING:
            raise MayimError("Transaction not active")
        if self._state in (
            TransactionState.COMMITTED,
            TransactionState.ROLLED_BACK,
        ):
            raise MayimError("Transaction already completed")

        try:
            # Rollback through executors (for compatibility with existing tests)
            for executor in self._executors:
                await executor.rollback(silent=True)

            self._state = TransactionState.ROLLED_BACK
            if self._context:
                self._context.state = TransactionState.ROLLED_BACK
        finally:
            # Clean up
            await self._cleanup()

    async def _timeout_monitor(self):
        """Monitor transaction timeout and auto-rollback if exceeded"""
        try:
            await asyncio.sleep(self._timeout)
            # Timeout exceeded, force rollback
            if self._state == TransactionState.ACTIVE:
                self._state = TransactionState.ROLLED_BACK
                if self._context:
                    self._context.state = TransactionState.ROLLED_BACK
        except asyncio.CancelledError:
            # Normal case - transaction completed before timeout
            pass

    def _check_timeout(self):
        """Check if transaction has timed out"""
        if self._timeout is not None and self._start_time is not None:
            elapsed = time() - self._start_time
            if elapsed > self._timeout:
                return True
        return False

    async def _cleanup(self):
        """Clean up transaction resources"""
        # Cancel timeout task
        if self._timeout_task and not self._timeout_task.done():
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except asyncio.CancelledError:
                pass

        if self._context:
            await self._context.cleanup()

        if self._stack:
            await self._stack.__aexit__(None, None, None)

        if self._token:
            try:
                _global_transaction.reset(self._token)
            except ValueError:
                # Token was reset in different context (e.g., timeout task)
                pass
            self._token = None

    async def __aenter__(self):
        """Context manager entry - automatically begins transaction"""
        await self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - commits or rolls back based on exception"""
        if self._state != TransactionState.ACTIVE:
            return False

        if exc_type is None:
            # No exception, commit
            try:
                await self.commit()
            except Exception:
                # Commit failed, ensure cleanup
                if self._state == TransactionState.ACTIVE:
                    await self._cleanup()
                    self._state = TransactionState.ROLLED_BACK
                raise
        else:
            # Exception occurred, rollback
            try:
                await self.rollback()
            except Exception:
                # Rollback failed, ensure cleanup
                if self._state == TransactionState.ACTIVE:
                    await self._cleanup()
                    self._state = TransactionState.ROLLED_BACK

        return False  # Don't suppress the exception

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get transaction metrics (optional feature).

        Returns:
            Dictionary containing transaction metrics
        """
        return {
            "state": self._state.value,
            "executor_count": len(self._executors),
            "pool_count": (
                len(self._context.connections) if self._context else 0
            ),
        }


def get_global_transaction() -> Optional[GlobalTransactionContext]:
    """
    Get the current global transaction context if one exists.

    Returns:
        The current GlobalTransactionContext or None
    """
    return _global_transaction.get()
