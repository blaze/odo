from into.convert import convert, list_to_numpy, iterator_to_numpy_chunks
from into.chunks import chunks
from datashape import discover
from toolz import first
from collections import Iterator
import datetime
import datashape
import numpy as np
import pandas as pd

def test_basic():
    assert convert(tuple, [1, 2, 3]) == (1, 2, 3)


def test_array_to_set():
    assert convert(set, np.array([1, 2, 3])) == set([1, 2, 3])


def eq(a, b):
    c = a == b
    if isinstance(c, (np.ndarray, pd.Series)):
        c = c.all()
    return c


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


def test_list_to_numpy_on_tuples():
    data = [['a', 1], ['b', 2], ['c', 3]]
    ds = datashape.dshape('var * (string[1], int32)')
    x = list_to_numpy(data, dshape=ds)
    assert convert(list, x) == [('a', 1), ('b', 2), ('c', 3)]


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


def test_datetimes_persist():
    typs = [list, tuple, pd.Series, np.ndarray, tuple]
    L = [datetime.datetime.now()] * 3
    ds = discover(L)

    x = L
    for cls in typs:
        x = convert(cls, x)
        assert discover(x) == ds


def test_numpy_to_list_preserves_ns_datetimes():
    x = np.array([(0, 0)], dtype=[('a', 'M8[ns]'), ('b', 'i4')])

    assert convert(list, x) == [(datetime.datetime(1970, 1, 1, 0, 0), 0)]
