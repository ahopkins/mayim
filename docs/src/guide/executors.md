# Creating Executors

As stated [earlier](basics#what-is-an-executor), an `Executor` is the object that will be responsible for running your queries. Go back to the [earlier documentation](basics) about why the methods are usually empty and how to [link Executor methods](sqlfiles) to SQL files.

In this section, we will look at some more customized features of `Executors` and different ways to instantiate them.

## Loading and instantiating

Mayim provides a few flexible options for registering an `Executor`. This should generally be done as early in the application as possible. For example, if you are running a web server, then this should ideally happen **before** the server starts to accept requests and not inside of a request handler.

### Loading a class

The simplest method is to load the class definition of your executor into your `Mayim` definition.

```python
from some.location.my.app import MyExecutor

async def run():
    Mayim(
        executors=[MyExecutor],
        ...
    )
```

But, if you need to, you can also instantiate `Mayim` without the `executors` argument and come back to load your `Executor` later.

```python
from some.location.my.app import MyExecutor

async def run():
    mayim = Mayim(...)
    ...
    mayim.load(executors=[MyExecutor])
```

Feel free to mix and match as needed.

```python
from some.location.my.app.users import UserExecutor
from some.location.my.app.products import ProductExecutor
from some.location.my.app.vendors import VendorExecutor

async def run():
    mayim = Mayim(executors=[UserExecutor])
    ...
    mayim.load(executors=[ProductExecutor, VendorExecutor])
```

### Loading an instance

In both the `Mayim` constructor and the `Mayim.load` method, you can also pass an `Executor` instance.

```python
from some.location.my.app.users import UserExecutor
from some.location.my.app.products import ProductExecutor
from some.location.my.app.vendors import VendorExecutor

async def run():
    user_executor = UserExecutor()
    product_executor = ProductExecutor()
    vendor_executor = VendorExecutor()
    Mayim(executors=[user_executor], ...)
    ...
    mayim.load(executors=[product_executor, vendor_executor])
```


### Global registration with `@register`

You also have the option of wrapping your `Executor` instance with `@register`. This will automatically add the `Executor` to the registry without having to manually load it later.

```python
from mayim import PostgresExecutor, register

@register
class MyExecutor(PostgresExecutor):
    async def select_something(self) -> Something:
        ...
```

Later on in your application, when you create `Mayim`, you will not need to explicitly pass the `Executor` class or instance.

```python
async def run():
    Mayim(...)
```

::: warning
Be careful with this method. If you use it you may need to pat close attention to your import ordering. That is because you will need to either: (1) instantiate `Mayim`, or (2) run `mayim.load` sometime after the `Executor` has been imported.

If you are using `@register` and your queries are not running as expected, a first place to check is to make sure that your imports are properly ordered.
:::

## Fetching an `Executor`

Anytime after an `Executor` has been loaded, you can fetch an executable instance:

```python
from mayim import Mayim
from some.location.my.app import MyExecutor

executor = Mayim.get(MyExecutor)
```

This is a very helpful pattern to allow you to access `Executor` instances in just about any part of your appication that you need to.

::: warning
Be careful about import ordering here. Although the example above places `executor` in the global scope, that is not well advised. You are much better off placing it inside of some function that will be called by your application to avoid import and run time errors. For example, inside of a web handler endpoint.

```python
@app.get("/foo")
async def handler(request: Request):
    executor = Mayim.get(MyExecutor)
    ...
```
:::

## In-line SQL queries

Sometimes you may decide that you do not want to have to [load SQL from files](sqlfiles). In this case, you can define the SQL in your Python code.

```python
from mayim import PostgresExecutor, sql

class ItemExecutor(PostgresExecutor):
    @sql(
        """SELECT *
        FROM items
        WHERE item_id = $item_id;
        """
    )
    async def select_item(self, item_id: int) -> Item:
        ...
```

## Dynamic queries and raw `execute`

What if you need to generate some SQL query and not use one that is predefined? Mayim provides access to a lower-level API for this purpose. You should pass your generated SQL query to `execute`.

```python
class CityExecutor(PostgresExecutor):
    async def select_city_by_id(self, city_id: int) -> City:
        query = """
            SELECT *
            FROM city
            WHERE id = $city_id
        """
        return await self.execute(query, city_id=city_id, as_list=False)
```

*FYI - `as_list` defaults to `False`. It is shown here just as an example that you may need to be explicit about passing this argument if you expect to return a `list`*

## Low level `run_sql`

What if you need are not sure at run time what model to return? What if you need to dynamically determine what should be hydrated? Mayim also provides a lower-level API for running the SQL, and then hydrating it with a given model.

```python
class CityExecutor(PostgresExecutor):
    async def select_city_by_id(self, city_id: int):
        query = """
            SELECT *
            FROM city
            WHERE id = $city_id
        """
        results = await self.run_sql(query, city_id=city_id, as_list=False)
        return self.hydrator.hydrate(results, City)
```

*FYI - `as_list` defaults to `False`. It is shown here just as an example that you may need to be explicit about passing this argument if you expect to return a `list`*

## Custom pools

Sometimes, you may find the need for an executor to be linked to a different database pool than other executors. This might be particularly helpful if you have multiple databases to query.

```python
from mayim.interface.postgres import PostgresPool
from some.location.my.app.users import UserExecutor
from some.location.my.app.products import ProductExecutor
from some.location.my.app.vendors import VendorExecutor

async def run():
    vendor_pool = PostgresPool("postgres://user@vendor.db:5432/db")
    user_executor = UserExecutor()
    product_executor = ProductExecutor()
    vendor_executor = VendorExecutor(pool=vendor_pool)
    Mayim(executors=[user_executor], dsn="postgres://user@main.db:5432/db")
    ...
    mayim.load(executors=[product_executor, vendor_executor])
```

## Custom hydrators

There is more information about [creating hydrators](hydrators) coming up next. But first, you should know that you can create your own custom hydrators that only operate on a specific `Executor`.

```python
from somewhere import CityHydrator

class CityExecutor(Executor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...

async def run():
    city_executor = CityExecutor(hydrator=CityHydrator())
    Mayim(executors=[city_executor], ...)
```
