from typing import AsyncContextManager, Optional, Type

from psycopg import AsyncConnection

from mayim.exception import MayimError
from mayim.interface.base import BaseInterface


class LazyPool(BaseInterface):
    _singleton = None
    _derivative: Optional[Type[BaseInterface]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._derivative = None
        return cls._singleton

    def _setup_pool(self):
        ...

    def _populate_connection_args(self):
        ...

    def _populate_dsn(self):
        ...

    async def open(self):
        ...

    async def close(self):
        ...

    def connection(
        self, timeout: Optional[float] = None
    ) -> AsyncContextManager[AsyncConnection]:
        ...

    def set_derivative(self, interface_class: Type[BaseInterface]) -> None:
        self._derivative = interface_class

    def derive(self) -> BaseInterface:
        if not self._derivative:
            raise MayimError("No interface available to derive")
        return self._derivative(dsn=self.full_dsn)
