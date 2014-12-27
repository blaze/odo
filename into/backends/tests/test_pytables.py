import numpy as np
import datashape as ds
import pytest
from contextlib import contextmanager

from into import into, cleanup
from into.utils import tmpfile

tb = pytest.importorskip('tables')
from into.backends.pytables import PyTables, discover

@contextmanager
def ensure_clean_file(ext='.h5',mode='w'):
    """ create and close a store """
    with tmpfile(ext) as filename:
        try:
            f = tb.open_file(filename, mode=mode)
            yield f
        finally:
            cleanup(f)

@contextmanager
def ensure_clean_store(*args, **kwargs):
    """ create and close a store """
    try:
        t = PyTables(*args, **kwargs)
        yield t
    finally:
        cleanup(t)

@pytest.yield_fixture
def table_file(arr):
    with ensure_clean_file() as f:
        d = f.create_table('/', 'title',  arr)
        d.close()
        yield f.filename

@pytest.fixture(scope='module')
def dt_arr():
    now = np.datetime64('now').astype('datetime64[us]')
    raw_dt_data = [(1, 'Alice', 100, now),
                   (2, 'Bob', -200, now),
                   (3, 'Charlie', 300, now),
                   (4, 'Denis', 400, now),
                   (5, 'Edith', -500, now)]

    return np.array(raw_dt_data, dtype=np.dtype([('id', 'i8'),
                                                 ('name', 'S7'),
                                                 ('amount', 'f8'),
                                                 ('date', 'M8[ms]')]))


@pytest.yield_fixture
def dt_table(dt_arr):
    class Desc(tb.IsDescription):
        id = tb.Int64Col(pos=0)
        name = tb.StringCol(itemsize=7, pos=1)
        amount = tb.Float64Col(pos=2)
        date = tb.Time64Col(pos=3)

    non_date_types = list(zip(['id', 'name', 'amount'], ['i8', 'S7', 'f8']))

    # has to be in microseconds as per pytables spec
    dtype = np.dtype(non_date_types + [('date', 'M8[us]')])
    rec = dt_arr.astype(dtype)

    # also has to be a floating point number
    dtype = np.dtype(non_date_types + [('date', 'f8')])
    rec = rec.astype(dtype)
    rec['date'] /= 1e6

    with ensure_clean_file() as f:
        d = f.create_table('/', 'dt', description=Desc)
        d.append(rec)
        d.close()
        yield f.filename


def test_read(table_file):

    with ensure_clean_store(path=table_file, datapath='/title') as t:
        shape = t.shape
        assert shape == (5,)

def test_write_no_dshape(table_file):
    with pytest.raises(ValueError):
        PyTables(path=table_file, datapath='/write_this')

def test_write_with_dshape(table_file):

    f = tb.open_file(table_file, mode='a')
    try:
        assert '/write_this' not in f
    finally:
        f.close()
        del f

    # create our table
    dshape = '{id: int, name: string[7, "ascii"], amount: float32}'
    with ensure_clean_store(path=table_file, datapath='/write_this', dshape=dshape) as t:
        shape = t.shape
        assert shape == (0,)
        filename = t._v_file.filename
        assert filename == table_file

@pytest.mark.xfail(reason="Poor datetime support")
def test_table_into_ndarray(dt_table, dt_arr):
    with ensure_clean_store(dt_table, '/dt') as t:
        res = into(np.ndarray, t)
        for k in res.dtype.fields:
            lhs, rhs = res[k], dt_arr[k]
            if (issubclass(np.datetime64, lhs.dtype.type) and
                issubclass(np.datetime64, rhs.dtype.type)):
                lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
            assert np.array_equal(lhs, rhs)

def test_ndarray_into_table(dt_table, dt_arr):
    dtype = ds.from_numpy(dt_arr.shape, dt_arr.dtype)
    with ensure_clean_store(dt_table, '/out', dtype) as t:
        res = into(np.ndarray, into(t, dt_arr, filename=dt_table, datapath='/out'))
        for k in res.dtype.fields:
            lhs, rhs = res[k], dt_arr[k]
            if (issubclass(np.datetime64, lhs.dtype.type) and
                issubclass(np.datetime64, rhs.dtype.type)):
                lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
            assert np.array_equal(lhs, rhs)

@pytest.mark.xfail(reason="Poor datetime support")
def test_datetime_discovery(dt_table, dt_arr):
    with ensure_clean_store(dt_table, '/dt') as t:
        lhs, rhs = map(discover, (t, dt_arr))
        assert lhs == rhs

def test_node_discover(dt_table):
    with ensure_clean_store(dt_table, '/') as root:
        result = discover(root)
        expected = ds.dshape("""{ dt: 5 * {id: int64,
        name: string[7, "A"],
        amount: float64,
        date: float64}}""")

        assert result == expected.measure
