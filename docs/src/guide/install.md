# Installation

Mayim supports both **Postgres**, **MySQL**, and **SQLite**. More data sources may be included in the future.

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
