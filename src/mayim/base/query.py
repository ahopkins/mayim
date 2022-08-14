from abc import ABC


class Query(ABC):
    __slots__ = ()
    name: str
    text: str
