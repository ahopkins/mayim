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
    print(await executor.select_all_people())


asyncio.run(run())
```

## Documentation

The docs: [ahopkins.github.io/mayim](https://ahopkins.github.io/mayim/guide/)

## Framework support

Out of the box, Mayim comes with extensions to support Quart, Sanic, and Starlette applications. Checkout the docs for more info.
