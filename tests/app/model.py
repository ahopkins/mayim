from dataclasses import dataclass


@dataclass
class Foo:
    bar: str


@dataclass
class Item:
    item_id: int
    name: str
