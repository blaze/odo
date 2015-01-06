
from contextlib import contextmanager
from collections import Iterator
import numpy as np
import pytest
import os

tb = pytest.importorskip('tables')

from datashape import dshape
from into import into, convert, resource, cleanup, chunks, drop
from into.utils import tmpfile

from into.backends.hdfstore import HDFStore, discover
from pandas import DataFrame, date_range, read_hdf, concat
from pandas.util.testing import assert_frame_equal


@pytest.fixture
def new_file(tmpdir):
    return str(tmpdir / 'foo.h5')


@pytest.yield_fixture
def hdf_file(df):
    with tmpfile('.h5') as filename:
        df.to_hdf(
            filename, 'title', mode='w', format='table', data_columns=True)
        yield filename


@pytest.yield_fixture
def hdf_file2(df2):
    with tmpfile('.h5') as filename:
        df2.to_hdf(filename, 'dt', mode='w', format='table', data_columns=True)
        yield filename


@pytest.yield_fixture
def hdf_file3():
    data3 = DataFrame(np.random.randn(10, 10), columns=['c%02d' % i
                                                        for i in range(10)])
    with tmpfile('.h5') as filename:
        data3.to_hdf(filename, 'dt', mode='w', format='table',
                     data_columns=True)
        yield filename


@pytest.yield_fixture()
def hdf_multi_nodes_file(df, df2):
    with tmpfile('.h5') as filename:
        df.to_hdf(filename, 'df', mode='w', format='table',
                  data_columns=True)
        df2.to_hdf(filename, 'df2', format='table',
                   data_columns=True)
        yield filename


def test_file_discover(hdf_multi_nodes_file):

    r = resource(hdf_multi_nodes_file)

    # smoke
    str(r)

    result = discover(r)
    expected = dshape("{ '/df'  : { table : 5 * { index : int64, amount : int64, id : int64, name : string[7, 'A'] } }, \
                         '/df2' : { table : 5 * { index : int64, amount : int64, id : int64, name : string[7, 'A'], date : int64 } } }")
    assert str(result) == str(expected)


def test_node_discover(hdf_multi_nodes_file):

    r = resource(hdf_multi_nodes_file)
    result = discover(r.get_table('df'))
    expected = dshape(
        "5 * {index: int64, amount: int64, id: int64, name: string[7, 'A']}")
    assert str(result) == str(expected)


def test_read(hdf_file):

    with HDFStore(path=hdf_file, datapath='/title') as t:
        shape = t.shape
        assert shape == (5,)


def test_write_no_dshape(new_file):

    with pytest.raises(ValueError):
        HDFStore(path=new_file, datapath='/write_this')


def test_write_with_dshape(new_file):

    dshape = '{id: int, name: string[7, "ascii"], amount: float32}'
    with HDFStore(path=new_file, datapath='/write_this',
                  dshape=dshape) as t:
        shape = t.shape
        assert t.parent.filename == new_file
        assert shape == (0,)


def test_table_into_dataframe(hdf_file2):

    with HDFStore(hdf_file2, '/dt') as t:
        res = into(DataFrame, t)
        assert_frame_equal(res, read_hdf(hdf_file2, 'dt'))


def test_table_into_dataframe_columns(hdf_file2):

    with HDFStore(hdf_file2, '/dt') as t:
        res = into(DataFrame, t, columns=['id', 'amount'])
        expected = read_hdf(hdf_file2, 'dt', columns=['id', 'amount'])
        assert_frame_equal(res, expected)


def test_table_into_dataframe_columns_large_ncols(hdf_file3):
    # efficient selection of columns

    with HDFStore(hdf_file3, '/dt') as t:

        for n in range(1, 3):
            cols = ['c%02d' % i for i in range(n)]
            res = into(DataFrame, t, columns=cols)
            expected = read_hdf(hdf_file3, 'dt', columns=cols)
            assert_frame_equal(res, expected)


def test_table_into_dataframe_where_no_columns(hdf_file2):

    with HDFStore(hdf_file2, '/dt') as t:
        res = into(DataFrame, t, where='amount>=300')
        expected = read_hdf(hdf_file2, 'dt', where='amount>=300')
        assert_frame_equal(res, expected)


def test_table_into_dataframe_where_and_columns(hdf_file2):

    with HDFStore(hdf_file2, '/dt') as t:
        res = into(DataFrame, t, where='amount>=300',
                   columns=['id', 'amount'])
        expected = read_hdf(hdf_file2, 'dt', where='amount>=300',
                            columns=['id', 'amount'])
        assert_frame_equal(res, expected)


def test_table_into_chunks_dataframe(hdf_file3):

    with HDFStore(hdf_file3, '/dt') as t:

        expected = read_hdf(hdf_file3, 'dt')
        for cs in [1, 5, 10]:
            res = into(chunks(DataFrame), t, chunksize=cs)
            res = concat(list(iter(res.data)), axis=0)

            assert_frame_equal(res, expected)


def test_dataframe_into_table(hdf_file2, new_file):

    expected = read_hdf(hdf_file2, 'dt')
    dshape = discover(expected)

    t = HDFStore(path=new_file, datapath='/write_this',
                 dshape=dshape)
    t = into(t, expected)

    res = read_hdf(new_file, 'write_this')
    assert_frame_equal(res, expected)


def test_dataframe_into_table_append(hdf_file2, new_file):

    expected = read_hdf(hdf_file2, 'dt')
    dshape = discover(expected)

    t = HDFStore(path=new_file, datapath='/write_this',
                 dshape=dshape)

    # clean store
    assert t.shape[0].val == 0
    t = into(t, expected)
    assert t.shape[0] == dshape.shape[0].val

    # append to the existing
    t = into(t, expected)
    assert t.shape[0] == 2 * dshape.shape[0].val

    res = read_hdf(new_file, 'write_this')
    assert_frame_equal(res, concat([expected, expected]))

    # make sure that we are still indexed
    with t as ot:
        assert ot.table.autoindex


def test_dataframe_into_table_append_chunks(hdf_file2, new_file):

    df = read_hdf(hdf_file2, 'dt')
    totality = concat([df] * 3)
    dshape = discover(totality)

    t = HDFStore(path=new_file, datapath='/write_this',
                 dshape=dshape)
    res = into(t, chunks(DataFrame)([df] * 3))
    assert_frame_equal(into(DataFrame, t), totality)

    res = read_hdf(new_file, 'write_this')
    assert_frame_equal(res, totality)


def test_into_iterator(hdf_file2):

    # this 'works', but you end up with an iterator on a closed file
    result = into(Iterator, hdf_file2 + '::dt')

    # the resource must remain open
    with resource(hdf_file2 + '::dt') as r:
        result = into(Iterator, r)
        assert_frame_equal(
            concat(list(iter(result))), read_hdf(hdf_file2, 'dt'))


def test_into_hdf5(df2, tmpdir):

    # test multi-intos for HDF5 types
    target1 = str(tmpdir / 'foo.h5')
    target2 = str(tmpdir / 'foo2.h5')

    # need to specify a datapath / uri here
    with pytest.raises(ValueError):
        into(target1, df2)

    into(target1, df2, datapath='/data')
    result = read_hdf(target1, 'data')
    assert_frame_equal(df2, result)

    r1 = resource(target1, datapath='/data')
    r2 = resource(target2, datapath='/data', dshape=discover(r1))
    into(r2, r1)

    result = read_hdf(target2, 'data')
    assert_frame_equal(df2, result)

    result = into(DataFrame, target2, datapath='/data')
    assert_frame_equal(df2, result)

    # append again
    into(target2, target1, datapath='/data')
    result = read_hdf(target2, 'data')
    assert_frame_equal(concat([df2, df2]), result)


def test_into_hdf52(df2, tmpdir):

    # test multi-intos for HDF5 types
    target1 = str(tmpdir / 'foo.h5')
    target2 = str(tmpdir / 'foo2.h5')

    # need to specify a datapath / uri here
    with pytest.raises(ValueError):
        into(target1, df2)

    into(target1 + '::df2', df2)
    result = read_hdf(target1, 'df2')
    assert_frame_equal(df2, result)

    into(target2 + '::df2', target1 + '::df2')
    result = read_hdf(target2, 'df2')
    assert_frame_equal(df2, result)

    result = into(DataFrame, target2 + '::df2')
    assert_frame_equal(df2, result)

    # store->store is invalid
    with pytest.raises(ValueError):
        into(target2, target1)

    # append again
    into(target2 + '::df2', target1 + '::df2')
    result = read_hdf(target2, 'df2')
    assert_frame_equal(concat([df2, df2]), result)


def test_into_other(tmpdir):

    target1 = str(tmpdir / 'foo.h5')
    uri = target1 + '::/data'

    data = [('Alice', 1), ('Bob', 2), ('Charlie', 3)]

    # cannot be constructed w/o a user specified datashape
    with pytest.raises(ValueError):
        into(uri, data)

    into(uri, data, dshape=dshape("3 * {id: string, amount: int64}"))
    result = into(DataFrame, uri)
    assert_frame_equal(result, DataFrame(data, columns=['id', 'amount']))


def test_drop(hdf_multi_nodes_file):

    assert os.path.exists(hdf_multi_nodes_file)
    r = resource(hdf_multi_nodes_file)
    drop(r)

    assert not os.path.exists(hdf_multi_nodes_file)


def test_drop_table(hdf_multi_nodes_file):

    r = resource(hdf_multi_nodes_file + '::df')
    drop(r)
    r = resource(hdf_multi_nodes_file)
    assert '/df' not in r


def test_contains(hdf_multi_nodes_file):

    r = resource(hdf_multi_nodes_file)
    assert '/df2' in r
    assert '/df' in r
    assert '/foo' not in r

    assert set(r.keys()) == set(['/df', '/df2'])


def test_into_return(df2, tmpdir):

    target = str(tmpdir / 'foo.h5')
    uri = target + '::df'

    # need a datapath
    with pytest.raises(ValueError):
        into(target, df2)

    result = into(uri, df2)
    assert result.dialect == 'HDFStore'

    result = into(uri, uri)
    assert result.dialect == 'HDFStore'

    result = into(DataFrame, uri)
    assert_frame_equal(result, concat([df2, df2]))
