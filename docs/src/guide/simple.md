# A full simple example

Now that you know how to [install](install), [run](basics), and [write SQL files](sqlfiles), we can make a complete working example.

This is what our directory should look like:

```
.
├── basic.py
├── docker-compose.yml
└── queries
    └── select_all_cities.sql
```

## Run a database

For this example, we will use the sample [`aa8y/postgres-dataset:world`](https://hub.docker.com/r/aa8y/postgres-dataset) database. Create a `docker-compose.yaml` file like this:

```yaml
# ./docker-compose.yml
version: "3.8"
services:
  db-postgres:
    image: aa8y/postgres-dataset:world
    ports:
      - 5432:5432
    command: ["postgres", "-c", "log_statement=all"]
```

And then run it:

```
docker-compose up
```

## Write some SQL

```sql
-- ./queries/select_all_cities.sql
SELECT *
FROM city
LIMIT $limit OFFSET $offset;
```

## Create your application

```python
# ./basic.py
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


async def run():
    executor = CityExecutor()
    Mayim(dsn="postgres://postgres:postgres@localhost:5432/world")
    print(await executor.all_cities())


asyncio.run(run())
```

## Run your application

In a different terminal session than where the database is running, you can now run your application.

```
$ python basic.py
[City(id=1, name='Kabul', countrycode='AFG', district='Kabol', population=1780000), City(id=2, name='Qandahar', countrycode='AFG', district='Qandahar', population=237500), City(id=3, name='Herat', countrycode='AFG', district='Herat', population=186800), City(id=4, name='Mazar-e-Sharif', countrycode='AFG', district='Balkh', population=127800)]
```
