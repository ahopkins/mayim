# Introduction

## :droplet: What is Mayim?

The simplest way to describe it would be to call it a **one-way ORM**. That is to say that it does *not* craft SQL statements for you. Think of it as **BYOQ** (Bring Your Own Query).

## :droplet: Why?

I have nothing against ORMs, truthfully. They serve a great purpose and can be the right tool for the job in many situations. I just prefer not to use them where possible. Instead, I would rather **have control of my SQL statements**.

The typical tradeoff though is that there is more work needed to hydrate from SQL queries to objects. Mayim aims to solve that.

## :droplet: How?

There are two parts to it:

1. Write some SQL files in a location that Mayim can access at startup
1. Create an `Executor` that defines the query parameters that will be passed to your SQL

Here is a real simple example:

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

This example should be complete and run as is. Of course, you would need to run a Postgres instance somewhere and set the `dsn` string appropriately. Other than that, this should be all set.

Let's continue on to see how we can install it.
