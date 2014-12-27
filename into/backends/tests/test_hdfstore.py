
from contextlib import contextmanager
import numpy as np
import pytest

from into import into, convert, resource
from into.utils import tmpfile

tb = pytest.importorskip('tables')
from into.backends.hdfstore import HDFStore, discover
from into import chunks
from pandas import DataFrame, date_range, read_hdf, concat
from pandas.util.testing import assert_frame_equal


@pytest.fixture
def new_file(tmpdir):
    return str(tmpdir / 'foo.h5')

@pytest.yield_fixture
def hdf_file(df):
    with tmpfile('.h5') as filename:
        df.to_hdf(filename,'title',mode='w',format='table',data_columns=True)
        yield filename

@pytest.yield_fixture
def hdf_file2(df2):
    with tmpfile('.h5') as filename:
        df2.to_hdf(filename,'dt',mode='w',format='table',data_columns=True)
        yield filename


@pytest.yield_fixture(scope='module')
def hdf_file3():
    data3 = DataFrame(np.random.randn(10, 10), columns=['c%02d' % i
                                                        for i in range(10)])
    with tmpfile('.h5') as filename:
        data3.to_hdf(filename, 'dt', mode='w', format='table',
                     data_columns=True)
        yield filename


@contextmanager
def ensure_clean_store(*args, **kwargs):
    try:
        t = HDFStore(*args, **kwargs)
        yield t
    finally:
        t.parent.close()


class TestHDFStore(object):

    def test_read(self, hdf_file):
        with ensure_clean_store(path=hdf_file, datapath='/title') as t:
            shape = t.shape
            assert shape == (5,)

    def test_write_no_dshape(self, new_file):
        with pytest.raises(ValueError):
            HDFStore(path=new_file, datapath='/write_this')

    def test_write_with_dshape(self, new_file):

        dshape = '{id: int, name: string[7, "ascii"], amount: float32}'
        with ensure_clean_store(path=new_file, datapath='/write_this',
                                dshape=dshape) as t:
            shape = t.shape
            assert t.parent.filename == new_file
            assert shape == (0,)

    def test_table_into_dataframe(self, hdf_file2):

        with ensure_clean_store(hdf_file2, '/dt') as t:
            res = into(DataFrame, t)
            assert_frame_equal(res, read_hdf(hdf_file2, 'dt'))

    def test_table_into_dataframe_columns(self, hdf_file2):

        with ensure_clean_store(hdf_file2, '/dt') as t:
            res = into(DataFrame, t, columns=['id', 'amount'])
            expected = read_hdf(hdf_file2, 'dt', columns=['id', 'amount'])
            assert_frame_equal(res, expected)

    def test_table_into_dataframe_columns_large_ncols(self, hdf_file3):
        # efficient selection of columns

        with ensure_clean_store(hdf_file3, '/dt') as t:

            for n in range(1, 3):
                cols = ['c%02d' % i for i in range(n)]
                res = into(DataFrame, t, columns=cols)
                expected = read_hdf(hdf_file3, 'dt', columns=cols)
                assert_frame_equal(res, expected)

    def test_table_into_dataframe_where_no_columns(self, hdf_file2):

        with ensure_clean_store(hdf_file2, '/dt') as t:
            res = into(DataFrame, t, where='amount>=300')
            expected = read_hdf(hdf_file2, 'dt', where='amount>=300')
            assert_frame_equal(res, expected)

    def test_table_into_dataframe_where_and_columns(self, hdf_file2):

        with ensure_clean_store(hdf_file2, '/dt') as t:
            res = into(DataFrame, t, where='amount>=300',
                       columns=['id', 'amount'])
            expected = read_hdf(hdf_file2, 'dt', where='amount>=300',
                                columns=['id', 'amount'])
            assert_frame_equal(res, expected)

    def test_table_into_chunks_dataframe(self, hdf_file3):

        with ensure_clean_store(hdf_file3, '/dt') as t:

            expected = read_hdf(hdf_file3, 'dt')
            for cs in [1, 5, 10]:
                res = into(chunks(DataFrame), t, chunksize=cs)
                res = concat(list(res.data()), axis=0)

                assert_frame_equal(res, expected)

    def test_dataframe_into_table(self, hdf_file2, new_file):

        expected = read_hdf(hdf_file2, 'dt')
        dshape = discover(expected)

        with ensure_clean_store(path=new_file, datapath='/write_this',
                                dshape=dshape) as t:
            t = into(t, expected)

            res = read_hdf(new_file, 'write_this')
            assert_frame_equal(res, expected)

    def test_dataframe_into_table_append(self, hdf_file2, new_file):

        expected = read_hdf(hdf_file2, 'dt')
        dshape = discover(expected)

        with ensure_clean_store(path=new_file, datapath='/write_this',
                                dshape=dshape) as t:

            # clean store
            assert t.nrows == 0
            t = into(t, expected)
            assert t.nrows == dshape.shape[0].val

            # append to the existing
            t = into(t, expected)
            assert t.nrows == 2 * dshape.shape[0].val

            res = read_hdf(new_file, 'write_this')
            assert_frame_equal(res, concat([expected, expected]))

            # make sure that we are still indexed
            assert t.table.autoindex

    def test_dataframe_into_table_append_chunks(self, hdf_file2, new_file):

        df = read_hdf(hdf_file2, 'dt')
        totality = concat([df] * 3)
        dshape = discover(totality)

        with ensure_clean_store(path=new_file, datapath='/write_this',
                                dshape=dshape) as t:
            into(t, chunks(DataFrame)([df] * 3))

            res = read_hdf(new_file, 'write_this')
            assert_frame_equal(res, totality)

    def test_into_hdf5(self, df2, tmpdir):

        #### FIXME ####
        # when we have resource cleanup for strings, then can
        # fix this up

        # test multi-intos for HDF5 types
        target1 = str(tmpdir / 'foo.h5')
        target2 = str(tmpdir / 'foo2.h5')

        t1 = resource(target1,datapath='/data',dshape=discover(df2))
        into(t1, df2)
        t1.parent.close()
        result = read_hdf(target1, 'data')
        assert_frame_equal(df2, result)

        t1 = resource(target1,datapath='/data')
        t2 = resource(target2,datapath='/data',dshape=discover(t1))
        into(t2, t1)
        t1.parent.close()
        t2.parent.close()
        result = read_hdf(target2, 'data')
        assert_frame_equal(df2, result)

        t2 = resource(target2,datapath='/data')
        result = into(DataFrame, t2)
        t2.parent.close()
        assert_frame_equal(df2, result)

        # append again
        t1 = resource(target1,datapath='/data')
        t2 = resource(target2,datapath='/data')
        into(t2, t1)
        t1.parent.close()
        t2.parent.close()
        result = read_hdf(target2, 'data')
        assert_frame_equal(concat([df2,df2]), result)
