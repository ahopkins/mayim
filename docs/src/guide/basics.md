# Basics

The general idea is that you write raw SQL queries, and Mayim takes care of running those queries and hydrating the results into Python objects. Let's start by creating a file called `basic.py`:

```python
import asyncio
from mayim import Mayim

async def run():
    mayim = Mayim(...)


asyncio.run(run())
```

The `Mayim` class takes a few parameters.

- `executors`: a list of `Executor` classes or instances
- `dsn`: the DSN to a DB instance (instead of `pool`)
- `pool`: a connection pool (instead of `dsn`)
- `hydrator`: a custom fallback hydrator (optional)

## What is an `Executor`?

We will get into it in [more detail later](executors), but the `Executor` is the object that will be responsible for running your queries. It is the one you will spend the most time interacting with.

You need to:

- **subclass** `Executor` (more likely you want one of its subclasses: `PostgresExecutor`, `MysqlExecutor`, `SQLiteExecutor`);
- create **method definitions** that match the names of your SQL statements (yes, those methods will likely be empty as seen in the snippet below);
- name the **arguments** that will be injected into the query; and
- provide the model you want to be returned as the **return annotation** (or annotate it as a `Dict` if that is what you want back).


```python
from typing import List
from mayim import PostgresExecutor
from dataclasses import dataclass


@dataclass
class City:
    city_id: int
    name: str


class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...
```

**Usually**, your executor will have a bunch of empty methods. That is because Mayim will automatically generate the code needed to run the SQL statement and return the object specified in the return annotation.

In this case, since `select_all_cities` is an empty method, Mayim will try to execute the SQL query called `./queries/select_all_cities.sql` (see [more on writing SQL files](sqlfiles)). Then, it will try and turn the result into a list of `City` objects because of the method's return annotation.

## Empty `Executor` methods

In its basic form, your `Executor` methods should be empty. That means any of the following are acceptable:

:::: tabs
::: tab Ellipsis
```python
class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...
```
:::
::: tab pass
```python
class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        pass
```
:::
::: tab docstring
```python
class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        """Fetch all cities"""
```
:::
::::
