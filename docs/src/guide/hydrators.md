# Custom Hydrators

A `Hydrator` is an object that is responsible for turning raw data into a **single** instance of some model. Out of the box, Mayim works with `dataclasses` and other packages (like Pydantic) where the following pattern is acceptable:

```python
instance = model(**data)
```

If you need to support other types of models, or add other business logic to the hydration process, then you can create custom hydrators.

## For an Executor

In the [last section](executors#custom-hydrators), we showed that it is possible to attach a `Hydrator` to an `Executor`.

```python
city_executor = CityExecutor(hydrator=CityHydrator())
```

But, what could the `CityHydrator` look like?

```python
from typing import Any, Dict, Type
from dataclasses import dataclass

@dataclass
class City:
    id: int
    name: str
    countrycode: str
    district: str
    population: int


class CityHydrator(Hydrator):
    def hydrate(
        self, data: Dict[str, Any], model: Type[object] = City
    ) -> City:
        data["population"] = round(data["population"] / 1_000_000, 2)
        return super().hydrate(data, model)
```

In this simple example, rather than returning the actual `population` in the database, we will return it in units of 1 million people.

As you can see, the `hydrate` method could return _ANYTHING_ you want it to. So if you need to support some other kind of model, all you need to do is create a hydrator that knows how to turn a `Dict[str, Any]` into the object type of your choice.

## As an async method

Sometimes you may decide that you need `hydrate` to be an `async` method. That is acceptable. Notice how we use `async def hydate` in the below example.

```python
class CityExecutor(PostgresExecutor):
    async def select_city_by_id(self, city_id: int) -> City:
        ...

class CountryExecutor(PostgresExecutor):
    async def select_country_by_country_code(
        self, country_code: str
    ) -> Country:
        ...

class CountryHydrator(Hydrator):
    def __init__(self, city_executor: CityExecutor):
        self.city_executor = city_executor

    async def hydrate(
        self, data: Dict[str, Any], model: Type[Country] = Country
    ) -> Country:
        capital = data.pop("capital")
        data["capital"] = await self.city_executor.select_city_by_id(capital)
        return super().hydrate(data, Country)

async def run():
    city_executor = CityExecutor()
    country_executor = CountryExecutor(hydrator=CountryHydrator(city_executor))
    Mayim.load(
        executors=[
            city_executor,
            country_executor,
        ]
    )
    ...
    print(await country_executor.select_country_by_country_code("ISR"))
```

::: warning
This example is illustrative, but potentially dangerous. By running another `Executor` inside of `hydrate` we are running two queries for every one. If you intend to do this, make sure you know about **N+1** operations.

Honestly, this is actually somewhat illustrative of *exactly* why this library exists. You can create **more efficient** queries than what traditional ORMs can handle if you write raw SQL. Mayim then makes it easy to take all that hard work and hydrate them into objects. Go nuts and create as complicated a query as you need.
:::

## Fallback hydrator

What if you need to support some type of model that does not take keyword arguments in the constructor? Or, what if you need to perform some logic for every model? Mayim will also allow you to create a custom `Hydrator` on the `Mayim` instance instead of having to pass it to every `Executor` individually.

```python
class MyHydrator(Hydrator):
    ...

async def run()
    Mayim(hydrator=MyHydrator(), ...)
```
