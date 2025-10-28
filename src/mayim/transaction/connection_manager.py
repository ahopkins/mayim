import asyncio
import logging
from typing import Any, Dict

from .interfaces import ConnectionIsolationError, TransactionError

logger = logging.getLogger(__name__)


class TransactionConnectionManager:
    def __init__(self, transaction_id: str, timeout: float = 300.0):
        self.transaction_id = transaction_id
        self.timeout = timeout
        self._connections: Dict[Any, Any] = {}
        self._connection_contexts: Dict[Any, Any] = {}
        self._active = True

    async def get_connection(self, pool) -> Any:
        """Get an isolated connection for the given pool"""
        if not self._active:
            raise ConnectionIsolationError(
                f"Transaction {self.transaction_id} is not active"
            )

        if pool not in self._connections:
            try:
                # Get a dedicated connection for this transaction
                connection_context = pool.connection()
                connection = await asyncio.wait_for(
                    connection_context.__aenter__(), timeout=self.timeout
                )
                self._connections[pool] = connection
                self._connection_contexts[pool] = connection_context
                logger.debug(
                    f"Created isolated connection for pool {pool} in "
                    f"transaction {self.transaction_id}"
                )
            except asyncio.TimeoutError:
                raise ConnectionIsolationError(
                    f"Timeout getting connection for transaction "
                    f"{self.transaction_id}"
                )
            except Exception as e:
                raise ConnectionIsolationError(
                    f"Failed to get connection for transaction "
                    f"{self.transaction_id}: {e}"
                )

        return self._connections[pool]

    async def execute_on_all(self, sql_command: str) -> None:
        """Execute SQL command on all managed connections"""
        if not self._connections:
            return

        errors = []

        for pool, connection in self._connections.items():
            try:
                await connection.execute(sql_command)
                logger.debug(
                    f"Executed '{sql_command}' on connection for pool {pool}"
                )
            except Exception as e:
                error_msg = (
                    f"Failed to execute '{sql_command}' on pool {pool}: {e}"
                )
                logger.error(error_msg)
                errors.append(error_msg)

        if errors:
            raise TransactionError(
                f"Command execution failed on some connections: "
                f"{'; '.join(errors)}"
            )

    async def cleanup(self) -> None:
        """Clean up all connections"""
        self._active = False

        for pool in self._connections:
            try:
                context = self._connection_contexts.get(pool)
                if context:
                    await context.__aexit__(None, None, None)
                logger.debug(f"Released connection for pool {pool}")
            except Exception as e:
                logger.warning(
                    f"Error releasing connection for pool {pool}: {e}"
                )

        self._connections.clear()
        self._connection_contexts.clear()
        logger.debug(
            f"Cleaned up connections for transaction {self.transaction_id}"
        )

    def inject_into_executor(self, executor) -> None:
        """Inject transaction connection into executor"""
        if (
            not hasattr(executor, "pool")
            or executor.pool not in self._connections
        ):
            return
        # Set the isolated connection on the pool
        connection = self._connections[executor.pool]
        executor.pool._set_transaction_connection(connection)
