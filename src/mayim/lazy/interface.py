from typing import Optional, Type

from mayim.base.interface import BaseInterface
from mayim.exception import MayimError


class LazyPool(BaseInterface):
    _singleton = None
    _derivative: Optional[Type[BaseInterface]]
    _derived_instance: Optional[BaseInterface]

    def __new__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = super().__new__(cls)
            cls._singleton._derivative = None
            cls._singleton._derived_instance = None
            cls._singleton._derivative_dsn = ""
            cls._singleton._min_size = 1
            cls._singleton._max_size = None
        return cls._singleton

    def _setup_pool(self): ...

    def _populate_dsn(self): ...

    def _populate_connection_args(self): ...

    async def open(self): ...

    async def close(self): ...

    def connection(self, timeout: Optional[float] = None): ...

    def set_derivative(self, interface_class: Type[BaseInterface]) -> None:
        self._derivative = interface_class

    def set_dsn(self, dsn: str) -> None:
        self._derivative_dsn = dsn

    def set_sizing(
        self, min_size: int = 1, max_size: Optional[int] = None
    ) -> None:
        self._min_size = min_size
        self._max_size = max_size

    def derive(self) -> BaseInterface:
        if not self._derivative:
            raise MayimError("No interface available to derive")
        if self._derived_instance:
            return self._derived_instance
        self._derived_instance = self._derivative(
            dsn=self._derivative_dsn,
            min_size=self._min_size,
            max_size=self._max_size,
        )
        return self._derived_instance
