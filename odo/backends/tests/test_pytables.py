from __future__ import absolute_import, division, print_function

import numpy as np
import datashape as ds
import pytest
tb = pytest.importorskip('tables')

from odo import into, odo
from odo.utils import tmpfile
from odo.backends.pytables import PyTables, discover
import os


try:
    f = tb.open_file('import-tables-test.hdf5', mode='w')
    f.close()
    if os.path.exists('import-tables-test.hdf5'):
        os.remove('import-tables-test.hdf5')
except tb.exceptions.HDF5ExtError as e:
    pytest.skip('Cannot write file, error:\n%s' % e)


x = np.array([(1, 'Alice', 100),
              (2, 'Bob', -200),
              (3, 'Charlie', 300),
              (4, 'Denis', 400),
              (5, 'Edith', -500)],
             dtype=[('id', '<i8'), ('name', 'S7'), ('amount', '<i8')])


@pytest.yield_fixture
def tbfile():
    with tmpfile('.h5') as filename:
        f = tb.open_file(filename, mode='w')
        d = f.create_table('/', 'title',  x)
        d.close()
        f.close()
        yield filename


now = np.datetime64('now').astype('datetime64[us]')
raw_dt_data = [(1, 'Alice', 100, now),
               (2, 'Bob', -200, now),
               (3, 'Charlie', 300, now),
               (4, 'Denis', 400, now),
               (5, 'Edith', -500, now)]


dt_data = np.array(raw_dt_data, dtype=np.dtype([('id', 'i8'),
                                                ('name', 'S7'),
                                                ('amount', 'f8'),
                                                ('date', 'M8[ms]')]))


@pytest.yield_fixture
def dt_tb():
    class Desc(tb.IsDescription):
        id = tb.Int64Col(pos=0)
        name = tb.StringCol(itemsize=7, pos=1)
        amount = tb.Float64Col(pos=2)
        date = tb.Time64Col(pos=3)

    non_date_types = list(zip(['id', 'name', 'amount'], ['i8', 'S7', 'f8']))

    # has to be in microseconds as per pytables spec
    dtype = np.dtype(non_date_types + [('date', 'M8[us]')])
    rec = dt_data.astype(dtype)

    # also has to be a floating point number
    dtype = np.dtype(non_date_types + [('date', 'f8')])
    rec = rec.astype(dtype)
    rec['date'] /= 1e6
    with tmpfile('.h5') as filename:
        f = tb.open_file(filename, mode='w')
        d = f.create_table('/', 'dt', description=Desc)
        d.append(rec)
        d.close()
        f.close()
        yield filename


class TestPyTablesLight(object):

    def test_read(self, tbfile):
        t = PyTables(path=tbfile, datapath='/title')
        shape = t.shape
        t._v_file.close()
        assert shape == (5,)

    def test_write_no_dshape(self, tbfile):
        with pytest.raises(ValueError):
            PyTables(path=tbfile, datapath='/write_this')

    def test_write_with_dshape(self, tbfile):
        f = tb.open_file(tbfile, mode='a')
        try:
            assert '/write_this' not in f
        finally:
            f.close()
            del f

        # create our table
        dshape = '{id: int, name: string[7, "ascii"], amount: float32}'
        t = PyTables(path=tbfile, datapath='/write_this', dshape=dshape)
        shape = t.shape
        filename = t._v_file.filename
        t._v_file.close()

        assert filename == tbfile
        assert shape == (0,)

    @pytest.mark.xfail(reason="Poor datetime support")
    def test_table_into_ndarray(self, dt_tb):
        t = PyTables(dt_tb, '/dt')
        res = into(np.ndarray, t)
        try:
            for k in res.dtype.fields:
                lhs, rhs = res[k], dt_data[k]
                if (issubclass(np.datetime64, lhs.dtype.type) and
                        issubclass(np.datetime64, rhs.dtype.type)):
                    lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
                assert np.array_equal(lhs, rhs)
        finally:
            t._v_file.close()

    def test_ndarray_into_table(self, dt_tb):
        dtype = ds.from_numpy(dt_data.shape, dt_data.dtype)
        t = PyTables(dt_tb, '/out', dtype)
        try:
            res = into(
                np.ndarray, into(t, dt_data, filename=dt_tb, datapath='/out'))
            for k in res.dtype.fields:
                lhs, rhs = res[k], dt_data[k]
                if (issubclass(np.datetime64, lhs.dtype.type) and
                        issubclass(np.datetime64, rhs.dtype.type)):
                    lhs, rhs = lhs.astype('M8[us]'), rhs.astype('M8[us]')
                assert np.array_equal(lhs, rhs)
        finally:
            t._v_file.close()

    @pytest.mark.xfail(reason="Poor datetime support")
    def test_datetime_discovery(self, dt_tb):
        t = PyTables(dt_tb, '/dt')
        lhs, rhs = map(discover, (t, dt_data))
        t._v_file.close()
        assert lhs == rhs

    def test_node_discover(self, dt_tb):
        root = PyTables(dt_tb, '/')
        result = discover(root)
        expected = ds.dshape("""{dt: 5 * {id: int64,
                                          name: string[7, "A"],
                                          amount: float64,
                                          date: float64}}""")
        assert result == expected.measure
        root._v_file.close()

    def test_no_extra_files_around(self, dt_tb):
        """ check the context manager auto-closes the resources """
        assert not len(tb.file._open_files)


def test_pytables_to_csv():
    ndim = 2
    with tmpfile('.h5') as fn:
        h5file = tb.open_file(fn, mode='w', title="Test Array")
        h5file.create_array('/', "test", np.zeros((ndim, ndim), dtype=float))
        h5file.close()
        with tmpfile('csv') as csv:
            t = odo('pytables://%s::/test' % fn, csv)
            assert odo(t, list) == [(0.0, 0.0), (0.0, 0.0)]
