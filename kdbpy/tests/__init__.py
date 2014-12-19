import pandas.util.testing as tm


def assert_series_equal(left, right, check_name=True, check_dtype=True,
                        check_index_type=True, check_series_type=True,
                        check_exact=True):
    if check_name:
        assert left.name == right.name
    tm.assert_series_equal(left, right, check_dtype=check_dtype,
                           check_index_type=check_index_type,
                           check_series_type=check_series_type,
                           check_exact=check_exact)
