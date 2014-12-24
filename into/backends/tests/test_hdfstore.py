
from contextlib import contextmanager
import numpy as np
import datashape as ds
import pytest

from into import into
from into.utils import tmpfile

tb = pytest.importorskip('tables')
from into.backends.hdfstore import HDFStore, discover
from into import chunks
from pandas import DataFrame, date_range, read_hdf, concat
from pandas.util.testing import assert_frame_equal

@pytest.fixture
def new_file(tmpdir):
    return str(tmpdir / 'foo.h5')

@pytest.fixture
def data():
    return DataFrame({ 'id' : range(5),
                       'name' : ['Alice','Bob','Charlie','Denis','Edith'],
                       'amount' : [100,-200,300,400,-500] })

@pytest.yield_fixture
def hdf_file(data):
    with tmpfile('.h5') as filename:
        data.to_hdf(filename,'title',format='table',data_columns=True)
        yield filename

@pytest.fixture
def data2(data):
    data2 = data.copy()
    data2['date'] = date_range('20130101 09:00:00',freq='s',periods=len(data))
    return data2

@pytest.yield_fixture
def hdf_file2(data2):
    with tmpfile('.h5') as filename:
        data2.to_hdf(filename,'dt',format='table',data_columns=True)
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
        with ensure_clean_store(path=new_file, datapath='/write_this', dshape=dshape) as t:
            shape = t.shape
            assert t.parent.filename == new_file
            assert shape == (1,)

    def test_table_into_dataframe(self, hdf_file2):

        with ensure_clean_store(hdf_file2, '/dt') as t:
            res = into(DataFrame, t)
            assert_frame_equal(res, read_hdf(hdf_file2,'dt'))

    def test_dataframe_into_table(self, hdf_file2, new_file):

        expected = read_hdf(hdf_file2,'dt')
        dshape = discover(expected)

        with ensure_clean_store(path=new_file, datapath='/write_this', dshape=dshape) as t:
            into(t, expected)

            res = read_hdf(new_file,'write_this')
            assert_frame_equal(res, expected)

    def test_dataframe_into_table_append(self, hdf_file2, new_file):

        expected = read_hdf(hdf_file2,'dt')
        dshape = discover(expected)

        with ensure_clean_store(path=new_file, datapath='/write_this', dshape=dshape) as t:

            # clean store
            assert t.nrows == 1
            into(t, expected)
            assert t.nrows == dshape.shape[0].val

            # append to the existing
            into(t, expected)
            assert t.nrows == 2*dshape.shape[0].val

            res = read_hdf(new_file,'write_this')
            assert_frame_equal(res, concat([expected,expected]))

            # make sure that we are still indexed
            assert t.table.autoindex

    def test_dataframe_into_table_append_chunks(self, hdf_file2, new_file):

        df = read_hdf(hdf_file2,'dt')
        totality = concat([df]*5)
        dshape = discover(totality)

        with ensure_clean_store(path=new_file, datapath='/write_this', dshape=dshape) as t:
            into(t, chunks(DataFrame)([df]*5))

            res = read_hdf(new_file,'write_this')
            assert_frame_equal(res, totality)
