# Introduction

## :droplet: What is Mayim?

The simplest way to describe it would be to call it a **one-way ORM**. That is to say that it does *not* craft SQL statements for you. But it does take your executed query results and map them back to objects.

Think of it as **BYOQ** (Bring Your Own Query) mapping utility.

You supply the query, it handles the execution and model hydration.

## :droplet: Why?

I have nothing against ORMs, truthfully. They serve a great purpose and can be the right tool for the job in many situations. I just prefer not to use them where possible. Instead, I would rather **have control of my SQL statements**.

The typical tradeoff though is that there is more work needed to hydrate from SQL queries to objects. Without an ORM, it is generally more difficult to maintain your code base as your schema changes. 

Mayim aims to solve that by providing an **elegant API** with typed objects and methods. Mayim fully embraces **type annotations** and encourages their usage.

## :droplet: How?

There are two parts to it:

1. Write some SQL in a location that Mayim can access at startup (_this can be in a decorator as shown below, or `.sql` files as seen later on_)
1. Create an `Executor` that defines the query parameters that will be passed to your SQL

Here is a real simple example:

```python
import asyncio
from mayim import Mayim, SQLiteExecutor, query
from dataclasses import dataclass


@dataclass
class Person:
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

This example should be complete and run as is.

Let's continue on to see how we can install it. :sunglasses:
