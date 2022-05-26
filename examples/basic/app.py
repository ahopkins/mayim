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
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...

    async def select_city_by_id(self, city_id) -> City:
        ...


async def run():
    Mayim(
        executors=[CityExecutor],
        dsn="postgres://postgres:postgres@localhost:5432/world",
    )
    executor = CityExecutor()
    print(await executor.select_all_cities())
    print(await executor.select_city_by_id(city_id=321))


asyncio.run(run())
