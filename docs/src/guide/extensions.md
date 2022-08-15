# Extensions

Out of the box, Mayim comes with extensions to hook into the lifecycles of commonly used async web frameworks.

## Quart

Mayim can attach to Quart using the `init_app` pattern and will handle setting up Mayim and the lifecycle events.

```python
from quart import Quart
from dataclasses import asdict
from typing import List
from mayim import PostgresExecutor
from model import City
from mayim.extension import QuartMayimExtension

app = Quart(__name__)


class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...


ext = QuartMayimExtension(
    executors=[CityExecutor],
    dsn="postgres://postgres:postgres@localhost:5432/world",
)
ext.init_app(app)


@app.route("/")
async def handler():
    executor = CityExecutor()
    cities = await executor.select_all_cities()
    return {"cities": [asdict(city) for city in cities]}
```


## Sanic

Mayim uses [Sanic Extensions](https://sanic.dev/en/plugins/sanic-ext/getting-started.html) v22.6+ to extend your [Sanic app](https://sanic.dev). It starts Mayim and provides [dependency injections](https://sanic.dev/en/plugins/sanic-ext/injection.html#injecting-services) into your routes of all of the executors.

```python
from typing import List
from dataclasses import asdict
from sanic import Sanic, Request, json
from sanic_ext import Extend
from mayim import Mayim
from mayim.executor import Executor
from mayim.extensions import SanicMayimExtension


class CityExecutor(Executor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...


app = Sanic(__name__)
Extend.register(
    SanicMayimExtension(
        executors=[CityExecutor], dsn="postgres://..."
    )
)


@app.get("/")
async def handler(request: Request, executor: CityExecutor):
    cities = await executor.select_all_cities()
    return json({"cities": [asdict(city) for city in cities]})
```


## Starlette

Mayim can attach to Starlette using the `init_app` pattern and will handle setting up Mayim and the lifecycle events.

```python
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from dataclasses import asdict
from typing import List
from mayim import PostgresExecutor
from model import City

from mayim.extension import StarletteMayimExtension
from mayim.extension.statistics import SQLCounterMixin


class CityExecutor(
    SQLCounterMixin,
    PostgresExecutor,
):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...


ext = StarletteMayimExtension(
    executors=[CityExecutor],
    dsn="postgres://postgres:postgres@localhost:5432/world",
)


async def handler(request):
    executor = CityExecutor()
    cities = await executor.select_all_cities()
    return JSONResponse({"cities": [asdict(city) for city in cities]})


app = Starlette(
    debug=True,
    routes=[
        Route("/", handler),
    ],
)
ext.init_app(app)
```
