from mayim.sql.query import ParamType
from mayim.sql.sqlite.query import SQLiteQuery


def test_detects_single_character_named_parameter():
    query = SQLiteQuery("select_value", "SELECT :a")

    assert query.param_type is ParamType.KEYWORD
