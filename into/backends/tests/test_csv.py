from __future__ import absolute_import, division, print_function

from into.backends.csv import CSV, append, convert, resource, csv_to_DataFrame
from into.utils import tmpfile, filetext
from into import into, append, convert, resource, discover
from collections import Iterator
import pandas as pd
import gzip
import datashape


def test_csv():
    with tmpfile('.csv') as fn:
        csv = CSV(fn, dshape='var * {name: string, amount: int}', delimiter=',')

        assert csv.dialect['delimiter'] == ','


def test_csv_append():
    with tmpfile('.csv') as fn:
        csv = CSV(fn)

        data = [('Alice', 100), ('Bob', 200)]
        append(csv, data)

        assert list(convert(Iterator, csv)) == data

        with open(fn) as f:
            s = f.read()
            assert 'Alice' in s
            assert '100' in s


def test_pandas_read():
    with filetext('Alice,1\nBob,2') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice', 1), ('Bob', 2)]
        assert list(df.columns) == ['name', 'amount']


def test_pandas_read_supports_datetimes():
    with filetext('Alice,2014-01-02\nBob,2014-01-03') as fn:
        ds = datashape.dshape('var * {name: string, when: date}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['name', 'when']
        assert df.dtypes['when'] == 'M8[ns]'


def test_pandas_read_supports_missing_integers():
    with filetext('Alice,1\nBob,') as fn:
        ds = datashape.dshape('var * {name: string, val: ?int32}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['name', 'val']
        assert df.dtypes['val'] == 'f4'


def test_pandas_read_supports_gzip():
    with filetext('Alice,1\nBob,2', open=gzip.open, extension='.csv.gz') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice', 1), ('Bob', 2)]
        assert list(df.columns) == ['name', 'amount']


def test_pandas_write():
    with tmpfile('.csv') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        data = [('Alice', 1), ('Bob', 2)]
        csv = CSV(fn, header=True)
        append(csv, data, dshape=ds)

        with open(fn) as f:
            assert 'name' in f.read()


def test_pandas_write_gzip():
    with tmpfile('.csv.gz') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        data = [('Alice', 1), ('Bob', 2)]
        csv = CSV(fn, header=True)
        append(csv, data, dshape=ds)

        with gzip.open(fn) as f:
            s = f.read()
            assert 'name' in s
            assert 'Alice,1' in s
