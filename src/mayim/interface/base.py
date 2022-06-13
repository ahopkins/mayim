from abc import ABC, abstractmethod
from collections import namedtuple
from typing import Optional
from urllib.parse import urlparse

from mayim.exception import MayimError

UrlMapping = namedtuple("UrlMapping", ("key", "cast"))


URLPARSE_MAPPING = {
    "hostname": UrlMapping("_host", str),
    "username": UrlMapping("_user", str),
    "password": UrlMapping("_password", str),
    "port": UrlMapping("_port", int),
    "path": UrlMapping("_db", lambda value: value.replace("/", "")),
}


class BaseInterface(ABC):
    scheme = "dummy"

    @abstractmethod
    def _setup_pool(self):
        ...

    @abstractmethod
    async def open(self):
        ...

    @abstractmethod
    async def close(self):
        ...

    @abstractmethod
    def connection(self, timeout: Optional[float] = None):
        ...

    def __init__(
        self,
        dsn: Optional[str] = None,
        host: Optional[int] = None,
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        db: Optional[int] = None,
    ) -> None:
        """DB class initialization.

        Args:
            dsn (str, optional): DB data source name
            host (str, optional): DB address URL or IP
            port (int, optional): DB port. Defaults to 6379
            password (str, optional): DB password
            db (int, optional): DB db. Defaults to 1
        """

        if dsn and host:
            raise MayimError("Cannot connect to DB using host and dsn")

        if not dsn:
            if port and (
                not isinstance(port, int) or port not in range(0, 65536)
            ):
                raise MayimError(
                    "port: must be an integer between 0 and 65535"
                )

            if host and (not isinstance(host, str) or not len(host) > 0):
                raise MayimError(
                    "host: must be a string at least 1 character long"
                )

        if password is not None and (
            not len(password) > 0 or not isinstance(password, str)
        ):
            raise MayimError(
                "password: must be a string at least 1 character long"
            )

        self._dsn = dsn
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = db
        self._full_dsn: Optional[str] = None

        self._populate_connection_args()
        self._populate_dsn()
        self._setup_pool()

    def __str__(self) -> str:
        return f"<{self.__class__.__name__} {self.dsn}>"

    def _populate_connection_args(self):
        parts = urlparse(self.dsn or "")
        for key, mapping in URLPARSE_MAPPING.items():
            if not getattr(self, mapping.key):
                value = getattr(parts, key)  # or TODO: make defaults
                setattr(self, mapping.key, mapping.cast(value))

    def _populate_dsn(self):
        self._dsn = (
            (
                f"{self.scheme}://{self.user}:...@"
                f"{self.host}:{self.port}/{self.db}"
            )
            if self.password
            else (
                f"{self.scheme}://{self.user}@"
                f"{self.host}:{self.port}/{self.db}"
            )
        )
        self._full_dsn = (
            (
                f"{self.scheme}://{self.user}:{self.password}@"
                f"{self.host}:{self.port}/{self.db}"
            )
            if self.password
            else self.dsn
        )

    @property
    def dsn(self):
        return self._dsn

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def user(self):
        return self._user

    @property
    def password(self):
        return self._password

    @property
    def db(self):
        return self._db

    @property
    def full_dsn(self):
        return self._full_dsn
