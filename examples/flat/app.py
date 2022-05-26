import asyncio
from typing import List
from mayim import Mayim, PostgresExecutor
from dataclasses import dataclass


@dataclass
class City:
    id: int
    name: str
    countrycode: str
    district: str
    population: int


class CityExecutor(PostgresExecutor):
    path = ""

    async def all_cities(self, limit: int = 4, offset: int = 0) -> List[City]:
        ...


async def run():
    executor = CityExecutor()
    Mayim(dsn="postgres://postgres:postgres@localhost:5432/world")
    print(await executor.all_cities())


asyncio.run(run())
