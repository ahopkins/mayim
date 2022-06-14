# Mayim

> The *NOT* ORM Python hydrator

**What is Mayim?**

The simplest way to describe it would be to call it a **one-way ORM**. That is to say that it does *not* craft SQL statements for you. Think of it as **BYOQ** (Bring Your Own Query).

**Why?**

I have nothing against ORMs, truthfully. They serve a great purpose and can be the right tool for the job in many situations. I just prefer not to use them where possible. Instead, I would rather have control of my SQL statements.

The typical tradeoff though is that there is more work needed to hydrate from SQL queries to objects. Mayim aims to solve that.

## Getting Started

```
pip install mayim[postgres]
```

```python
import asyncio
from typing import List
from mayim import Mayim, PostgresExecutor, sql
from dataclasses import dataclass

@dataclass
class Person:
    name: str

class PersonExecutor(PostgresExecutor):
    @sql("SELECT * FROM people LIMIT $limit OFFSET $offset")
    async def select_all_people(
        self, limit: int = 4, offset: int = 0
    ) -> List[Person]:
        ...

async def run():
    executor = PersonExecutor()
    Mayim(dsn="postgres://...")
    print(await executor.select_all_cities())


asyncio.run(run())
```

## Documentation

The docs: [ahopkins.github.io/mayim](https://ahopkins.github.io/mayim/guide/)

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
        executors=[CityExecutor], dsn="postgres://..."
    )
)


@app.get("/")
async def handler(request: Request, executor: CityExecutor):
    cities = await executor.select_all_cities()
    return json({"cities": [asdict(city) for city in cities]})
```
