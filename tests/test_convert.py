from mayim.convert import convert_sql_params


def test_converts_sql_params():
    sql = """
        SELECT *
        FROM sometable
        LIMIT $limit
        OFFSET $offset
    """
    expected = """
        SELECT *
        FROM sometable
        LIMIT %(limit)s
        OFFSET %(offset)s
    """
    converted = convert_sql_params(sql)

    assert converted == expected
