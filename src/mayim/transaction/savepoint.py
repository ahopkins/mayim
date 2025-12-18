"""
Savepoint implementation for nested transaction rollback points.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .interfaces import TransactionError

if TYPE_CHECKING:
    from .coordinator import TransactionCoordinator

logger = logging.getLogger(__name__)


class Savepoint:
    """
    A savepoint allows creating nested rollback points within a transaction.

    Supports PostgreSQL and MySQL. SQLite has limited savepoint support.
    """

    def __init__(self, name: str, coordinator: TransactionCoordinator):
        self.name = name
        self.coordinator = coordinator
        self._released = False

        logger.debug(
            f"Created savepoint {self.name} in transaction "
            f"{coordinator.transaction_id}"
        )

    async def rollback(self) -> None:
        """Rollback to this savepoint"""
        if self._released:
            raise TransactionError(f"Savepoint {self.name} already released")

        if not self.coordinator.is_active:
            raise TransactionError(
                f"Cannot rollback savepoint {self.name} - "
                f"transaction not active"
            )

        logger.debug(f"Rolling back to savepoint {self.name}")

        try:
            await self.coordinator._connection_manager.execute_on_all(
                f"ROLLBACK TO SAVEPOINT {self.name}"
            )
            logger.info(f"Successfully rolled back to savepoint {self.name}")
        except Exception as e:
            logger.error(f"Failed to rollback to savepoint {self.name}: {e}")
            raise TransactionError(
                f"Failed to rollback to savepoint {self.name}: {e}"
            ) from e

    async def release(self) -> None:
        """Release this savepoint (commits it and frees resources)"""
        if self._released:
            raise TransactionError(f"Savepoint {self.name} already released")

        if not self.coordinator.is_active:
            raise TransactionError(
                f"Cannot release savepoint {self.name} - "
                f"transaction not active"
            )

        logger.debug(f"Releasing savepoint {self.name}")

        try:
            await self.coordinator._connection_manager.execute_on_all(
                f"RELEASE SAVEPOINT {self.name}"
            )
            self._released = True

            # Remove from coordinator's savepoint tracking
            if self.name in self.coordinator._savepoints:
                del self.coordinator._savepoints[self.name]
        except Exception as e:
            logger.error(f"Failed to release savepoint {self.name}: {e}")
            raise TransactionError(
                f"Failed to release savepoint {self.name}: {e}"
            ) from e

    @property
    def is_released(self) -> bool:
        """Check if this savepoint has been released"""
        return self._released

    def __str__(self) -> str:
        status = "released" if self._released else "active"
        return f"<Savepoint {self.name} ({status})>"
