import re

CONVERT_PATTERN = re.compile(r"(\$([a-z0-9_]+))")


def convert_sql_params(query: str) -> str:
    return CONVERT_PATTERN.sub(r"%(\2)s", query, 0)
