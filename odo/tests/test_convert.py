from __future__ import absolute_import, division, print_function

import pytest
from odo.convert import (convert, list_to_numpy, iterator_to_numpy_chunks,
                         dataframe_to_chunks_dataframe, numpy_to_chunks_numpy,
                         chunks_dataframe_to_dataframe,
                         iterator_to_DataFrame_chunks)
from odo.chunks import chunks
from datashape import discover, dshape
from collections import Iterator
import datetime
import datashape
import numpy as np
import pandas as pd
import pandas.util.testing as tm


def test_basic():
    assert convert(tuple, [1, 2, 3]) == (1, 2, 3)


def test_array_to_set():
    assert convert(set, np.array([1, 2, 3])) == set([1, 2, 3])


def eq(a, b):
    c = a == b
    if isinstance(c, (np.ndarray, pd.Series)):
        c = c.all()
    return c


def test_Series_to_ndarray():
    assert eq(convert(np.ndarray, pd.Series([1, 2, 3]), dshape='3 * float64'),
              np.array([1.0, 2.0, 3.0]))
    assert eq(convert(np.ndarray, pd.Series(['aa', 'bbb', 'ccccc']),
                      dshape='3 * string[5, "A"]'),
              np.array(['aa', 'bbb', 'ccccc'], dtype='S5'))

    assert eq(convert(np.ndarray, pd.Series(['aa', 'bbb', 'ccccc']),
                      dshape='3 * ?string'),
              np.array(['aa', 'bbb', 'ccccc'], dtype='O'))


def test_Series_to_object_ndarray():
    ds = datashape.dshape('{amount: float64, name: string, id: int64}')
    expected = np.array([1.0, 'Alice', 3], dtype='object')
    result = convert(np.ndarray, pd.Series(expected), dshape=ds)
    np.testing.assert_array_equal(result, expected)


def test_Series_to_datetime64_ndarray():
    s = pd.Series(pd.date_range(start='now', freq='N', periods=10).values)
    expected = s.values
    result = convert(np.ndarray, s.values)
    np.testing.assert_array_equal(result, expected)


def test_set_to_Series():
    assert eq(convert(pd.Series, set([1, 2, 3])),
              pd.Series([1, 2, 3]))


def test_Series_to_set():
    assert convert(set, pd.Series([1, 2, 3])) == set([1, 2, 3])


def test_dataframe_and_series():
    s = pd.Series([1, 2, 3], name='foo')
    df = convert(pd.DataFrame, s)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ['foo']

    s2 = convert(pd.Series, df)
    assert isinstance(s2, pd.Series)

    assert s2.name == 'foo'


def test_iterator_and_numpy_chunks():
    c = iterator_to_numpy_chunks([1, 2, 3], chunksize=2)
    assert isinstance(c, chunks(np.ndarray))
    assert all(isinstance(chunk, np.ndarray) for chunk in c)

    c = iterator_to_numpy_chunks([1, 2, 3], chunksize=2)
    L = convert(list, c)
    assert L == [1, 2, 3]


def test_list_to_numpy():
    ds = datashape.dshape('3 * int32')
    x = list_to_numpy([1, 2, 3], dshape=ds)
    assert (x == [1, 2, 3]).all()
    assert isinstance(x, np.ndarray)

    ds = datashape.dshape('3 * ?int32')
    x = list_to_numpy([1, None, 3], dshape=ds)
    assert np.isnan(x[1])


def test_list_of_single_element_tuples_to_series():
    data = [(1,), (2,), (3,)]
    ds = datashape.dshape('3 * {id: int64}')
    result = convert(pd.Series, data, dshape=ds)
    expected = pd.Series([1, 2, 3], name='id')
    tm.assert_series_equal(result, expected)


def test_cannot_convert_to_series_from_more_than_one_column():
    data = [(1, 2), (2, 3), (3, 4)]
    ds = datashape.dshape('3 * {id: int64, id2: int64}')
    with pytest.raises(ValueError):
        convert(pd.Series, data, dshape=ds)


def test_list_to_numpy_on_tuples():
    data = [['a', 1], ['b', 2], ['c', 3]]
    ds = datashape.dshape('var * (string[1], int32)')
    x = list_to_numpy(data, dshape=ds)
    assert convert(list, x) == [('a', 1), ('b', 2), ('c', 3)]


def test_list_to_numpy_on_dicts():
    data = [{'name': 'Alice', 'amount': 100},
            {'name': 'Bob', 'amount': 200}]
    ds = datashape.dshape('var * {name: string[5], amount: int}')
    x = list_to_numpy(data, dshape=ds)
    assert convert(list, x) == [('Alice', 100), ('Bob', 200)]


def test_list_of_dicts_with_missing_to_numpy():
    data = [{'name': 'Alice', 'amount': 100},
            {'name': 'Bob'},
            {'amount': 200}]
    result = convert(np.ndarray, data)
    assert result.dtype.names == ('amount', 'name')
    expected = np.array([(100.0, 'Alice'),
                         (np.nan, 'Bob'),
                         (200.0, None)],
                        dtype=[('amount', 'float64'), ('name', 'O')])
    assert np.all((result == expected) |
                  ((result != result) & (expected != expected)))


def test_chunks_numpy_pandas():
    x = np.array([('Alice', 100), ('Bob', 200)],
                 dtype=[('name', 'S7'), ('amount', 'i4')])
    n = chunks(np.ndarray)([x, x])

    pan = convert(chunks(pd.DataFrame), n)
    num = convert(chunks(np.ndarray), pan)

    assert isinstance(pan, chunks(pd.DataFrame))
    assert all(isinstance(chunk, pd.DataFrame) for chunk in pan)

    assert isinstance(num, chunks(np.ndarray))
    assert all(isinstance(chunk, np.ndarray) for chunk in num)


def test_numpy_launders_python_types():
    ds = datashape.dshape('3 * int32')
    x = convert(np.ndarray, ['1', '2', '3'], dshape=ds)
    assert convert(list, x) == [1, 2, 3]


def test_numpy_asserts_type_after_dataframe():
    df = pd.DataFrame({'name': ['Alice'], 'amount': [100]})
    ds = datashape.dshape('1 * {name: string[10, "ascii"], amount: int32}')
    x = convert(np.ndarray, df, dshape=ds)
    assert discover(x) == ds


def test_list_to_dataframe_without_datashape():
    data = [('Alice', 100), ('Bob', 200)]
    df = convert(pd.DataFrame, data)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) != ['Alice', 100]
    assert convert(list, df) == data


def test_noop():
    assert convert(list, [1, 2, 3]) == [1, 2, 3]


def test_generator_is_iterator():
    g = (1 for i in range(3))
    L = convert(list, g)
    assert L == [1, 1, 1]


def test_list_of_lists_to_set_creates_tuples():
    assert convert(set, [[1], [2]]) == set([(1,), (2,)])


def test_list_of_strings_to_set():
    assert convert(set, ['Alice', 'Bob']) == set(['Alice', 'Bob'])


def test_datetimes_persist():
    typs = [list, tuple, np.ndarray, tuple]
    L = [datetime.datetime.now()] * 3
    ds = discover(L)

    x = L
    for cls in typs:
        x = convert(cls, x)
        assert discover(x) == ds


def test_numpy_to_list_preserves_ns_datetimes():
    x = np.array([(0, 0)], dtype=[('a', 'M8[ns]'), ('b', 'i4')])

    assert convert(list, x) == [(datetime.datetime(1970, 1, 1, 0, 0), 0)]


def test_numpy_to_chunks_numpy():
    x = np.arange(100)
    c = numpy_to_chunks_numpy(x, chunksize=10)
    assert isinstance(c, chunks(np.ndarray))
    assert len(list(c)) == 10
    assert eq(list(c)[0], x[:10])


def test_pandas_and_chunks_pandas():
    df = pd.DataFrame({'a': [1, 2, 3, 4], 'b': [1., 2., 3., 4.]})

    c = dataframe_to_chunks_dataframe(df, chunksize=2)
    assert isinstance(c, chunks(pd.DataFrame))
    assert len(list(c)) == 2

    df2 = chunks_dataframe_to_dataframe(c)
    tm.assert_frame_equal(df, df2)


def test_iterator_to_DataFrame_chunks():
    data = ((0, 1), (2, 3), (4, 5), (6, 7))
    df1 = pd.DataFrame(list(data))
    df2 = iterator_to_DataFrame_chunks(data, chunksize=2, add_index=True)
    df2 = pd.concat(df2, axis=0)
    tm.assert_frame_equal(df1, df2)
    df2 = convert(pd.DataFrame, data, chunksize=2, add_index=True)
    tm.assert_almost_equal(df1, df2)


def test_recarray():
    data = np.array([(1, 1.), (2, 2.)], dtype=[('a', 'i4'), ('b', 'f4')])
    result = convert(np.recarray, data)
    assert isinstance(result, np.recarray)
    assert eq(result.a, data['a'])

    result2 = convert(np.ndarray, data)
    assert not isinstance(result2, np.recarray)
    assert eq(result2, data)


def test_empty_iterator_to_chunks_dataframe():
    ds = dshape('var * {x: int}')
    result = convert(chunks(pd.DataFrame), iter([]), dshape=ds)
    data = convert(pd.DataFrame, result)
    assert isinstance(data, pd.DataFrame)
    assert list(data.columns) == ['x']


def test_empty_iterator_to_chunks_ndarray():
    ds = dshape('var * {x: int}')
    result = convert(chunks(np.ndarray), iter([]), dshape=ds)
    data = convert(np.ndarray, result)
    assert isinstance(data, np.ndarray)
    assert len(data) == 0
    assert data.dtype.names == ('x',)


def test_chunks_of_lists_and_iterators():
    L = [1, 2], [3, 4]
    cl = chunks(list)(L)
    assert convert(list, cl) == [1, 2, 3, 4]
    assert list(convert(Iterator, cl)) == [1, 2, 3, 4]
    assert len(list(convert(chunks(Iterator), cl))) == 2


def test_ndarray_to_df_preserves_field_names():
    ds = dshape('2 * {a: int, b: int}')
    arr = np.array([[0, 1], [2, 3]])
    # dshape explicitly sets field names.
    assert (convert(pd.DataFrame, arr, dshape=ds).columns == ['a', 'b']).all()
    # no dshape is passed.
    assert (convert(pd.DataFrame, arr).columns == [0, 1]).all()


def test_iterator_to_df():
    ds = dshape('var * int32')
    it = iter([1, 2, 3])
    df = convert(pd.DataFrame, it, dshape=ds)
    assert df[0].tolist() == [1, 2, 3]

    it = iter([1, 2, 3])
    df = convert(pd.DataFrame, it, dshape=None)
    assert df[0].tolist() == [1, 2, 3]

    it = iter([1, 2, 3])
    df = convert(pd.DataFrame, it)
    assert df[0].tolist() == [1, 2, 3]
