---
home: true
tagline: The NOT ORM Python hydrator
actionText: Learn more →
actionLink: /guide/
footer: Made with ❤️ and ☕️ by Adam Hopkins
features:
- title: Bring Your Own Query
  details: Hydrate your raw SQL queries into Python objects
- title: Fully typed
  details: Leverage the power of type annotations to drive your code development
- title: Async Enabled
  details: Build your application with your favorite async framework
---


## :droplet: Hydrate your SQL into Python objects

Given this structure:

```
./queries
├── queries
│   └── select_all_cities.sql
└── basic.py
```

And, with these files:

:::: tabs
::: tab select_all_cities
```sql
-- ./queries/select_all_cities.sql
SELECT *
FROM city
LIMIT $limit OFFSET $offset;
```
:::
::: tab basic
```python
# basic.py
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
    Mayim(
        executors=[CityExecutor],
        dsn="postgres://postgres:postgres@localhost:5432/world"
    )
    executor = CityExecutor()
    print(await executor.select_all_cities())


asyncio.run(run())
```
:::
::::

Your query is now complete :sunglasses:

```
[City(id=1, name='Kabul', countrycode='AFG', district='Kabol', population=1780000), City(id=2, name='Qandahar', countrycode='AFG', district='Qandahar', population=237500), City(id=3, name='Herat', countrycode='AFG', district='Herat', population=186800), City(id=4, name='Mazar-e-Sharif', countrycode='AFG', district='Balkh', population=127800)]
```
