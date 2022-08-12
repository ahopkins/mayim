import re

from mayim.exception import MayimError

DOLLAR_KEYWORD = re.compile(r"(\$([a-z][a-z0-9_]*))")
DOLLAR_POSITIONAL = re.compile(r"(\$(\d+))")


def convert_sql_params(
    query: str, positional_sub: str = r"%s", keyword_sub: str = r"%(\2)s"
) -> str:
    matches = 0
    if DOLLAR_KEYWORD.search(query):
        matches += 1
        query = DOLLAR_KEYWORD.sub(keyword_sub, query, 0)
    if DOLLAR_POSITIONAL.search(query):
        matches += 1
        query = DOLLAR_POSITIONAL.sub(positional_sub, query, 0)
    if matches > 1:
        raise MayimError(f"Could not properly convert SQL params {matches}")
    return query
