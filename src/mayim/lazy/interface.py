from typing import Optional, Type

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError


class LazyPool(BaseInterface):
    _singleton = None
    _derivative: Optional[Type[BaseInterface]]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._derivative = None
            cls._singleton._derivative_dsn = ""
        return cls._singleton

    def _setup_pool(self):
        ...

    def _populate_dsn(self):
        ...

    def _populate_connection_args(self):
        ...

    async def open(self):
        ...

    async def close(self):
        ...

    def connection(self, timeout: Optional[float] = None):
        ...

    def set_derivative(self, interface_class: Type[BaseInterface]) -> None:
        self._derivative = interface_class

    def set_dsn(self, dsn: str) -> None:
        self._derivative_dsn = dsn

    def derive(self) -> BaseInterface:
        if not self._derivative:
            raise MayimError("No interface available to derive")
        return self._derivative(dsn=self._derivative_dsn)
