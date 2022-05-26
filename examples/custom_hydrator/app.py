import asyncio
from typing import Any, Dict, List, Type
from mayim import Mayim
from mayim import PostgresExecutor
from dataclasses import dataclass

from mayim.hydrator import Hydrator


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


class CityHydrator(Hydrator):
    def hydrate(
        self, data: Dict[str, Any], model: Type[object] = City
    ) -> City:
        data["population"] = round(data["population"] / 1_000_000, 2)
        return super().hydrate(data, model)


async def run():
    executor = CityExecutor(hydrator=CityHydrator())
    mayim = Mayim(dsn="postgres://postgres:postgres@localhost:5432/world")
    mayim.load(executors=[executor])
    print(await executor.select_all_cities())


asyncio.run(run())
