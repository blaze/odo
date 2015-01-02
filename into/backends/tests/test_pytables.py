
import os
import numpy as np
import datashape as ds
import pytest
from contextlib import contextmanager
from collections import Iterator

from datashape import dshape
from into import into, cleanup, resource, drop
from into.utils import tmpfile

tb = pytest.importorskip('tables')
from into.backends.pytables import PyTables, discover


@pytest.fixture
def new_file(tmpdir):
    return str(tmpdir / 'foo.h5')


@pytest.yield_fixture
def table_file(arr):
    with tmpfile('.h5') as filename:
        f = tb.open_file(filename, mode='w')
        f.create_table('/', 'title',  arr)
        f.close()
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

    with tmpfile('.h5') as filename:
        f = tb.open_file(filename, mode='w')
        d = f.create_table('/', 'dt', description=Desc)
        d.append(rec)
        f.close()
        yield f.filename


@pytest.yield_fixture()
def hdf_multi_nodes_file(arr):
    with tmpfile('.h5') as filename:
        f = tb.open_file(filename, mode='w')
        f.create_table('/', 'arr',  arr)
        f.create_table('/', 'arr2',  arr)
        f.close()
        yield filename


def test_read(table_file):

    with PyTables(path=table_file, datapath='/title') as t:
        shape = t.shape
        assert shape == (5,)


def test_write_no_dshape(new_file):
    with pytest.raises(ValueError):
        PyTables(path=new_file, datapath='/write_this')


def test_write_with_dshape(new_file):

    # create our table
    dshape = '{id: int, name: string[7, "ascii"], amount: float32}'
    with PyTables(path=new_file, datapath='/write_this', dshape=dshape) as t:
        shape = t.shape
        assert shape == (0,)
        filename = t._v_file.filename
        assert filename == new_file


@pytest.mark.xfail(reason="Poor datetime support")
def test_table_into_ndarray(dt_table, dt_arr):
    with PyTables(dt_table, '/dt') as t:
        res = into(np.ndarray, t)
        for k in res.dtype.fields:
            lhs, rhs = res[k], dt_arr[k]
            if (issubclass(np.datetime64, lhs.dtype.type) and
                    issubclass(np.datetime64, rhs.dtype.type)):
                lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
            assert np.array_equal(lhs, rhs)


def test_ndarray_into_table(dt_table, dt_arr):

    dtype = ds.from_numpy(dt_arr.shape, dt_arr.dtype)
    with PyTables(dt_table, '/out', dtype) as t:
        res = into(
            np.ndarray, into(t, dt_arr, filename=dt_table, datapath='/out'))
        for k in res.dtype.fields:
            lhs, rhs = res[k], dt_arr[k]
            if (issubclass(np.datetime64, lhs.dtype.type) and
                    issubclass(np.datetime64, rhs.dtype.type)):
                lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
            assert np.array_equal(lhs, rhs)


@pytest.mark.xfail(reason="Poor datetime support")
def test_datetime_discovery(dt_table, dt_arr):
    with PyTables(dt_table, '/dt') as t:
        lhs, rhs = map(discover, (t, dt_arr))
        assert lhs == rhs


def test_node_discover(dt_table):
    with PyTables(dt_table, '/') as root:
        result = discover(root)
        expected = ds.dshape("""{ dt: 5 * {id: int64,
        name: string[7, "A"],
        amount: float64,
        date: float64}}""")

        assert result == expected.measure


def test_into_other(tmpdir):

    target1 = str(tmpdir / 'foo.h5')
    uri = 'pytables://' + target1 + '::/data'

    data = [('Alice', 1), ('Bob', 2), ('Charlie', 3)]
    ds = dshape("3 * {id: string[7, 'A'], amount: int64}")

    # cannot be constructed w/o a user specified datashape
    with pytest.raises(ValueError):
        into(uri, data)

    into(uri, data, dshape=ds)
    result = into(np.ndarray, uri)
    expected = into(np.ndarray, data, dshape=ds)
    assert np.array_equal(result, expected)


def test_into_iterator(table_file):

    uri = 'pytables://' + table_file + '::/title'
    expected = into(np.ndarray, uri)

    # this 'works', but you end up with an iterator on a closed file
    result = into(Iterator, uri)

    # the resource must remain open
    with resource(uri) as r:
        result = into(Iterator, r)
        assert np.array_equal(
            np.concatenate(list(iter(result))), expected)


def test_drop(hdf_multi_nodes_file):

    assert os.path.exists(hdf_multi_nodes_file)
    r = resource('pytables://' + hdf_multi_nodes_file)
    drop(r)

    assert not os.path.exists(hdf_multi_nodes_file)


def test_drop_table(hdf_multi_nodes_file):

    r = resource('pytables://' + hdf_multi_nodes_file + '::/arr')
    drop(r)
    r = resource('pytables://' + hdf_multi_nodes_file)
    assert '/arr' not in r


def test_contains(hdf_multi_nodes_file):

    r = resource('pytables://' + hdf_multi_nodes_file)
    assert '/arr2' in r
    assert '/arr' in r
    assert 'arr' in r
    assert '/foo' not in r

    assert set(r.keys()) == set(['/arr', '/arr2'])


def test_into_return(arr, tmpdir):

    target = str(tmpdir / 'foo.h5')
    uri = 'pytables://' + target + '::/arr'

    # need a datapath
    with pytest.raises(ValueError):
        into(target, arr)

    result = into(uri, arr)
    assert result.dialect == 'PyTables'

    result = into(uri, uri)
    assert result.dialect == 'PyTables'

    result = into(np.ndarray, uri)
    np.array_equal(result, np.concatenate([arr, arr]))
