# Working with Pydantic

## It just works :sunglasses:

Out of the box, Mayim works great with [Pydantic](https://pydantic-docs.helpmanual.io/).

To get started, just annotate your executors with a Pydantic model.


```python
import asyncio
from typing import List, Optional
from mayim import Mayim, SQLiteExecutor, sql
from pydantic import BaseModel


class Person(BaseModel):
    name: str


class PersonExecutor(SQLiteExecutor):
    @query("SELECT $name as name")
    async def select_person(self, name: str) -> Person:
        ...


async def run():
    executor = PersonExecutor()
    Mayim(db_path="./example.db")
    print(await executor.select_person(name="Adam"))


asyncio.run(run())
```

_(This script is complete, it should run "as is")_

## Nested models

But, that is kind of boring and really misses one of the great features of Pydantic: it's ability to hydrate nested data. Going back to one of our earlier examples, what if we wanted to retrieve some data that looked like this:

```python
class City(BaseModel):
    id: int
    name: str
    district: str
    population: int


class Country(BaseModel):
    code: str
    name: str
    continent: str
    region: str
    capital: City
```

Our goal is to query for some countries, but we also want to capture the nested city information and have it available as `country.capital`, which is shown in the models.

To do this, we can use SQL to generate rows that have nested JSON blobs as columns. When these queries are made, they will be output as nested `dict` objects that Pydantic will easily convert to our usable form.

We start by making a `.sql` file with our desired query:

```sql
-- ./queries/select_all_countries.sql
SELECT country.code,
    country.name,
    country.continent,
    country.region,
    (
        SELECT row_to_json(q)
        FROM (
                SELECT city.id,
                    city.name,
                    city.district,
                    city.population
            ) q
    ) capital
FROM country
    JOIN city ON country.capital = city.id
ORDER BY country.name ASC
LIMIT $limit OFFSET $offset;
```

Next, we make and run an executor like any other.

```python
class CountryExecutor(PostgresExecutor):
    async def select_all_countries(
        self, limit: int = 4, offset: int = 0
    ) -> List[Country]:
        ...


async def run():
    country_executor = CountryExecutor()
    Mayim(dsn="postgres://postgres:postgres@localhost:5432/world")
    print(
        await country_executor.select_all_countries(50_000_000)
    )
```

As you would expect, we now have nice nested models:

```python
[
    Country(
        code="AFG",
        name="Afghanistan",
        continent="Asia",
        region="Southern and Central Asia",
        capital=City(id=1, name="Kabul", district="Kabol", population=1780000),
    ),
    Country(
        code="ALB",
        name="Albania",
        continent="Europe",
        region="Southern Europe",
        capital=City(id=34, name="Tirana", district="Tirana", population=270000),
    ),
    Country(
        code="DZA",
        name="Algeria",
        continent="Africa",
        region="Northern Africa",
        capital=City(id=35, name="Alger", district="Alger", population=2168000),
    ),
    Country(
        code="ASM",
        name="American Samoa",
        continent="Oceania",
        region="Polynesia",
        capital=City(id=54, name="Fagatogo", district="Tutuila", population=2323),
    ),
]
```
