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

### Implied registration

On the frontpage example, we instantiated our executor before calling `Mayim`, and **never** explicitly loaded the class (or instance). This implie registration is fine *only if it is instantiated before the `Mayim` object*. Because of this "gotcha", this option is not the recommended approach.


### Global registration with `@register`

You also have the option of wrapping your `Executor` instance with `@register`. This will automatically add the `Executor` to the registry without having to manually load it later. This is more explicit (and therefore preferred) than implied registration.

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
Be careful with this method. If you use it you may need to pay close attention to your import ordering. That is because you will need to either: (1) instantiate `Mayim`, or (2) run `mayim.load` sometime after the `Executor` has been imported.

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
from mayim import PostgresExecutor, query

class ItemExecutor(PostgresExecutor):
    @query(
        """
        SELECT *
        FROM items
        WHERE item_id = $item_id;
        """
    )
    async def select_item(self, item_id: int) -> Item:
        ...
```

## Dynamic queries and raw `execute`

What if you need to generate some SQL and not use a predefined query? Mayim provides access to a lower-level API for this purpose. You should pass your generated SQL query to `execute`.

```python
class CityExecutor(PostgresExecutor):
    async def select_city(self, ident: int | str, by_id: bool) -> City:
        query = """
            SELECT *
            FROM city
        """
        if by_id:
            query += "WHERE id = $ident"
        else:
            query += "WHERE name = $ident"
        return await self.execute(
            query,
            as_list=False,
            allow_none=False,
            params={"ident": ident}
        )
```

*FYI - `as_list` defaults to `False`. It is shown here just as an example that you may need to be explicit about passing this argument if you expect to return a `list`. Once you have dropped down into executing your own code, you are responsible for telling Mayim if it needs to return a `list` or a single instance. Similarly, `allow_none` also defaults to `False` and is shown for demonstration purposes here.*

### Query fragments

What if you have some really big query fragments, or some fragment that needs to be used in a lot of places? For example, you might have a large select statement that you want to reuse. Wouldn't it be nice if you could define those in `.sql` files and compose them? Of course!

```python
class CityExecutor(PostgresExecutor):
    generic_prefix: str = "fragment_"

    async def select_city(self, ident: int | str, by_id: bool) -> City:
        query = self.get_query("fragment_select_city")
        if by_id:
            query += self.get_query("fragment_where_id")
        else:
            query += self.get_query("fragment_where_name")
        return await self.execute(query, params={"ident": ident})
```

## Low level `run_sql`

What if you need are not sure at run time what model to return? What if you need to dynamically determine what should be hydrated? Mayim also provides a lower-level API for running the SQL, and then hydrating it with a given model.

```python
class CityExecutor(PostgresExecutor):
    async def select_city_by_id(self, city_id: int):
        query = self.get_query()
        results = await self.run_sql(query.text, params={"city_id": city_id})
        return self.hydrator.hydrate(results, City)
```


## Fetching query

In the previous example, you may have noticed `query = self.get_query()`. This method allows you to fetch the predefined query that *would* have been executed. It is helpful in cases where you need to add some more custom logic to your method, but still want to preload your SQL from a `.sql` file.

When you call `get_query()` with no arguments, it will fetch the query that Mayim thinks should have been run. This is based upon the method name. You can explicitly pass it a name to retrieve that as seen in the [query fragments](#query-fragments) section.

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

### Method specific hydrator

You can also define a hydrator that will only be used for a single method. This is done by wrapping the method with the `@hydrator` decorator as shown here

```python
from mayim import Mayim, Executor, Hydrator, hydrator

class HydratorA(Hydrator):
    ...

class HydratorB(Hydrator):
   ...

class SomeExecuto(Executor):
    async def select_a(...) -> Something:
        ...

    @hydrator(HydratorB())
    async def select_b(...) -> Something:
        ...

Mayim(executors=[SomeExecutor(hydrator=HydratorA())])
```

### Fetching hydrators

Just like you can use `self.get_query()` to have access to the SQL that would run for a method, you can use `self.get_hydrator()` similarly. Let's rewrite that earlier example with a method-specific hydrator.

```python
from mayim import Mayim, PostgresExecutor, Hydrator, hydrator

class CityHydrator(Hydrator):
    ...

class CityExecutor(PostgresExecutor):
    @hydrator(CityHydrator())
    async def select_city_by_id(self, city_id: int) -> List[City]:
        query = self.get_query()
        hydrator = self.get_hydrator()
        results = await self.run_sql(query.text, params={"city_id": city_id})
        return [hydrator.hydrate(city, City) for city in results]
```

## Empty methods in STRICT mode

By default, Mayim will startup in STRICT mode. You can disable this at instantiation, or `load`:

```python
mayim = Mayim(..., strict=False)
# OR
mayim.load(..., strict=False)
```

What does it do, and why would you use it? When enabled, Mayim will raise an exception when it encounters an empty Executor method without a defined query. So, if you have a method without any code inside of it, without a `@query` decorator, and without a `.sql` file, you will receive an exception.

```python
# Loading this executor would raise an exception in STRICT mode
class CityExecutor(PostgresExecutor):
    async def select_query_does_not_exist(self) -> None:
        ...
```
