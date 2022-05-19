**⚠️ This project is still in **ALPHA DEVELOPMENT** and not yet released on PyPI**

# Mayim

> The *NOT* ORM hydrator

**What is Mayim?**

The simplest way to describe it would be to call it a **one-way ORM**. That is to say that it does *not* craft SQL statements for you. Think of it as **BYOQ** (Bring Your Own Query).

**Why?**

I have nothing against ORMs, truthfully. They serve a great purpose and can be the right tool for the job in many situations. I just prefer not to use them where possible. Instead, I would rather have control of my SQL statements.

The typical tradeoff though is that there is more work needed to hydrate from SQL queries to objects. Mayim aims to solve that.

## Getting Started

```
pip install mayim
```

> Currently, only Postgres support thru `psycopg3` is supported. That may change in the future. You will need to install that on your own:

```
pip install psycopg[binary]
```


## Using Mayim

The general idea is that you write raw SQL queries, and Mayim takes care of running those queries and hydrating the results into Python objects. Let's start by creating a file called `example.py`:

```python
import asyncio
from mayim import Mayim

async def run():
    mayim = Mayim(...)


asyncio.run(run())
```

We'll get to filling this file out in a moment, but first you need to create some SQL files.

### Writing SQL files

The easiest way to use Mayim is to create a directory called `./queries` and place SQL files in there. You could alternatively write the SQL inside your Python code, but that's messy and you do not get any nice syntax highlighting. by writing raw `.sql` files you can use all of the wonderful tools your IDE has to offer you.

In my example, we will be selecting some city information from a DB. My structure will look like this:

```
./queries
├── queries
│   └── select_all_cities.sql
└── basic.py
```

Creating `select_all_cities.sql`:

```sql
SELECT *
FROM city
LIMIT $limit OFFSET $offset;
```

So, there are a few things to point out here. Mayim is only going to load SQL files that start with one of the four (4) SQL verbs: 

- `select_<something>.sql`
- `create_<something>.sql`
- `update_<something>.sql`
- `delete_<something>.sql`

The other thing to notice is that rather than using number-based `$1` and `$2` parameters, we are using named-parameters. This is a convenience added by Mayim and psycopg.

### Creating Mayim objects

There are two main objects you need to worry about: `Executor` and `Hydrator`.

### Executor

The `Executor` is the object that will be responsible for running your queries. It is the one you will spend the most time interacting with. 

You need to:

1. subclass `Executor`;
1. create method definitions that match the names of the SQL files you want it to execute;
1. name the arguments that will be injected into the query; and
1. provide the model you want to be returned as the return annotation.

In or example, it will look like this:

```python
from typing import List
from mayim.executor import Executor
from dataclasses import dataclass


@dataclass
class City:
    id: int
    name: str
    countrycode: str
    district: str
    population: int


class CityExecutor(Executor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        """This is intentionally an empty function.

        We do not need anything more than:
        1. a docstring,
        2. "...", or
        3. "pass"

        That is because Mayim will automatically generate the coded needed to
        run the SQL statement and return the object specified in the return
        annotation.

        In this case: List[City]
        """
```

With this complete, you are done. Let's turn back to `basic.py and provide a full working example:

```python
import asyncio
from typing import List
from mayim import Mayim
from mayim.executor import Executor
from dataclasses import dataclass


@dataclass
class City:
    id: int
    name: str
    countrycode: str
    district: str
    population: int


class CityExecutor(Executor):
    async def select_all_cities(
        self, limit: int = 25, offset: int = 0
    ) -> List[City]:
        ...


async def run():
    mayim = Mayim(
        CityExecutor, dsn="postgres://postgres:postgres@localhost:5432/world"
    )
    executor = mayim.get(CityExecutor)
    print(await executor.select_all_cities())


asyncio.run(run())
```

In this example, we are creating the instance of the `CityExecutor` by calling `mayim.get`. You do not have to. You can alternatively use one of these options:

```python
executor = Mayim.get(CityExecutor)
executor = CityExecutor()
```

### Hydrator

The `Hydrator` is the object that is responsible for turning the  query results into an object. For our simple example that we just saw, the `Hydrator` was implicitly created. But what if you have some logic that you need to add? 

In the previous example, one of the fields we were fetching per city was population. What if we want to report that number in units of millions? We can accomplish this with a custom `Hydrator`.

A custom `Hydrator` has a `hydrate` method that takes two arguments:

1. a `dict` of key/value data, and
1. a model class.

```python
import asyncio
from typing import Any, Dict, List, Type
from mayim import Mayim
from mayim.executor import Executor
from dataclasses import dataclass

from mayim.hydrator import Hydrator


@dataclass
class City:
    id: int
    name: str
    countrycode: str
    district: str
    population: int


class CityExecutor(Executor):
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
    mayim = Mayim(dsn="postgres://postgres:postgres@localhost:5432/world")
    mayim.load(CityExecutor(hydrator=CityHydrator()))
    executor = mayim.get(CityExecutor)
    print(await executor.select_all_cities())


asyncio.run(run())
```

The hydrate method could alternatively be an `async def hydrate`. Consider the following example:

```python
class CountryHydrator(Hydrator):
    def __init__(self, city_executor: CityExecutor):
        self.city_executor = city_executor

    async def hydrate(self, raw: Dict[str, Any], model: Type[object]):
        capital = raw.pop("capital")
        raw["capital"] = await self.city_executor.select_city_by_id(capital)
        return super().hydrate(raw)
```

While something like the above could be possible (see `full_example.py`), just be careful about **N+1** operations.

## Coming soon: Sanic support

In v22.6, [Sanic Extensions](https://sanic.dev/en/plugins/sanic-ext/getting-started.html) will introduce a simplified API for adding custom extensions to your [Sanic app](https://sanic.dev). It will look something like this:

```python
from typing import List
from dataclasses import asdict
from sanic import Sanic, Request, json
from sanic_ext import Extend
from mayim import Mayim
from mayim.executor import Executor
from mayim.extensions import MayimExtension


class CityExecutor(Executor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...


app = Sanic(__name__)
Extend.register(
    MayimExtension(
        CityExecutor, dsn="postgres://postgres:postgres@localhost:5432/world"
    )
)


@app.get("/")
async def handler(request: Request, executor: CityExecutor):
    cities = await executor.select_all_cities()
    return json({"cities": [asdict(city) for city in cities]})
```
