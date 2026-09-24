# Installation

Mayim supports both **Postgres**, **MySQL**, **SQLite**, and **ClickHouse**. More data sources may be included in the future.

You can install Mayim using PIP:

```
pip install mayim
```

To get access to support for a specific data source, make sure you install the appropriate dependencies. You must install one of the following drivers.

## Postgres

Dependencies:
- [psycopg3](https://www.psycopg.org/psycopg3/)

Either install it independently:

```
pip install psycopg[binary]
```

Or, as a convenience:

```
pip install mayim[postgres]
```

## MySQL

Dependencies:
- [asyncmy](https://github.com/long2ice/asyncmy)

Either install it independently:

```
pip install asyncmy
```

Or, as a convenience:

```
pip install mayim[mysql]
```

## SQLite

Dependencies:
- [aiosqlite](https://github.com/omnilib/aiosqlite)

Either install it independently:

```
pip install aiosqlite
```

Or, as a convenience:

```
pip install mayim[sqlite]
```

## ClickHouse

Dependencies:
- [clickhouse-connect](https://github.com/ClickHouse/clickhouse-connect) (with the `[async]` extra, requires **Python 3.10+**)

Either install it independently:

```
pip install "clickhouse-connect[async]"
```

Or, as a convenience:

```
pip install mayim[clickhouse]
```

A few things to keep in mind when using ClickHouse:

- ClickHouse does not support interactive transactions. Including a `ClickhouseExecutor` in `Mayim.transaction(...)` (or calling `begin()`/`commit()` on it) raises an error. The no-argument `Mayim.transaction()` form automatically skips ClickHouse executors, and queries run on a ClickHouse executor inside another executor's transaction are not part of that transaction.
- Query parameters are bound client side using pyformat values. A literal `%` in a query that takes parameters must therefore be escaped as `%%` (unlike with Postgres).
- Methods that should not return anything (such as `INSERT` or DDL statements) should either have no return annotation or be annotated with `-> None` so they are routed through the driver's `command` method.
- The HTTP driver manages its own connection pool, so `min_size` has no effect; `max_size` maps onto the driver's connection limit.
