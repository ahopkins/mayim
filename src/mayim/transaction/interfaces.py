from enum import Enum

from mayim.exception import MayimError


class IsolationLevel(Enum):
    """SQL transaction isolation levels"""

    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


class TransactionError(MayimError):
    """Base exception for transaction errors"""

    pass


class TransactionTimeoutError(TransactionError):
    """Raised when transaction times out"""

    pass


class ConnectionIsolationError(TransactionError):
    """Raised when connection isolation fails"""

    pass


class SavepointNotSupportedError(TransactionError):
    """Raised when savepoints are not supported by the database"""

    pass
