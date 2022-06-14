# Writing SQL files

Have you ever tried writing SQL statements in Python code? Its not a pleasant experience. :anguished: Wouldn't it just be nicer if you could write `*.sql` files? Then those files could have all the nice features your IDE wants to offer with syntax highlighting, code execution, auto-suggestions, etc.

This part is not necessary, but to be honest is one of the really powerful features of Mayim so take advantage of it!

## Where to save your `.sql` files?

Your SQL files should be located in a directory relative to the [Executor](executors) that is going to run them.

```
.
├── queries
│   └── select_something.sql
└── my_executor.py
```

That means if you define an executor in `./my_executor.py`, then Mayim will look for SQL files in `./queries`. That is to say that it is looking based upon the parent diectory wherever the `Executor` is defined.

### Customizing the location

You can change the location for where these files are located. This is easily handled by setting `Executor.path` to the location where the SQL can be found.

#### Flat directory

What if, for example, you want the SQL files and executor all in the same directory?

```
.
├── my_executor.py
└── select_something.sql
```

```python
class MyExecutor(PostgresExecutor):
    path = ""

    async def select_something(self) -> Something:
        ...
```

#### Nested directory

How about changing the name of the directory from `./queries` to `./path/to/sql`?

```
.
├── path
│   └── to
│       └── sql
│           └── select_something.sql
└── my_executor.py
```

```python
class MyExecutor(PostgresExecutor):
    path = "./path/to/sql"

    async def select_something(self) -> Something:
        ...
```

## How to name your `.sql` files?

Mayim will look SQL files that start with one of the four (4) SQL verbs:

- `select_<something>.sql`
- `create_<something>.sql`
- `update_<something>.sql`
- `delete_<something>.sql`

As you can see, usually you will simply name your methods and the SQL files the same.

**Make sure you name your files properly.**

But, what if none of these names work for you? Mayim will also load any SQL files that are prefixed with a known prefix, for example: `mayim_<something>.sql`. Just note, if you do this then the `mayim_` prefix is pulled off of the method name.

```python
.
├── queries
│   └── mayim_something.sql
└── my_executor.py

```
```python
class MyExecutor(PostgresExecutor):
    async def something(self) -> Something:
        ...
```

In this case, Mayim sees that there is a method named: `something`. Therefore, it will look for `mayim_something.sql`.

## Parameter injection

Mayim will inject parameters from the `Executor` method into your SQL queries as named arguments. It follows a `$argument_name` pattern as shown here:

```sql
SELECT *
FROM city
LIMIT $limit
OFFSET $offset;
```

```python
class CityExecutor(PostgresExecutor):
    async def select_all_cities(
        self, limit: int = 4, offset: int = 0
    ) -> List[City]:
        ...
```

Notice how the `limit` and `offset` arguments in the method will be translated into `$limit` and `$offset` respectively.

Alternatively, you can use a positional style of query arguments:

```sql
SELECT *
FROM city
LIMIT $1
OFFSET $2;
```

If you do this, you will need to make sure that the order of the method arguments correspond to the positional numbers being used.
