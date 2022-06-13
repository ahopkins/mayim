import re

from mayim.exception import MayimError

DOLLAR_KEYWORD = re.compile(r"(\$([a-z][a-z0-9_]*))")
DOLLAR_POSITIONAL = re.compile(r"(\$([0-9_]+))")


def convert_sql_params(query: str) -> str:
    matches = 0
    if DOLLAR_KEYWORD.search(query):
        matches += 1
        query = DOLLAR_KEYWORD.sub(r"%(\2)s", query, 0)
    if DOLLAR_POSITIONAL.search(query):
        matches += 1
        query = DOLLAR_POSITIONAL.sub(r"%s", query, 0)
    if matches > 1:
        raise MayimError("Could not properly convert SQL params")
    return query
