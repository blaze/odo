from __future__ import absolute_import, division, print_function

import sys
import os
import pandas as pd
import gzip
import datashape
from datashape import Option, string
from collections import Iterator

from odo.backends.csv import (CSV, append, convert, resource,
        csv_to_DataFrame, CSV_to_chunks_of_dataframes, infer_header)
from odo.utils import tmpfile, filetext, filetexts, raises
from odo import (into, append, convert, resource, discover, dshape, Temp,
        chunks)
from odo.temp import _Temp
from odo.compatibility import unicode, skipif


def test_csv():
    with tmpfile('.csv') as fn:
        csv = CSV(fn, dshape='var * {name: string, amount: int}', delimiter=',')

        assert csv.dialect['delimiter'] == ','


def test_csv_append():
    with tmpfile('.csv') as fn:
        csv = CSV(fn, has_header=False)

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


@skipif(os.name == 'nt')
def test_pandas_read_supports_gzip():
    with filetext('Alice,1\nBob,2', open=gzip.open,
                  mode='wt', extension='.csv.gz') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice', 1), ('Bob', 2)]
        assert list(df.columns) == ['name', 'amount']


def test_pandas_read_supports_read_csv_kwargs():
    with filetext('Alice,1\nBob,2') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_DataFrame(csv, dshape=ds, usecols=['name'])
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice',), ('Bob',)]


def test_pandas_write():
    with tmpfile('.csv') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        data = [('Alice', 1), ('Bob', 2)]
        csv = CSV(fn, has_header=True)
        append(csv, data, dshape=ds)

        with open(fn) as f:
            assert 'name' in f.read()

        # Doesn't write header twice
        append(csv, data, dshape=ds)
        with open(fn) as f:
            s = f.read()
            assert s.count('name') == 1


def test_pandas_writes_header_by_default():
    with tmpfile('.csv') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        data = [('Alice', 1), ('Bob', 2)]
        csv = CSV(fn)
        append(csv, data, dshape=ds)

        with open(fn) as f:
            assert 'name' in f.read()


@skipif(sys.version_info[0] == 3)
def test_pandas_write_gzip():
    with tmpfile('.csv.gz') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        data = [('Alice', 1), ('Bob', 2)]
        csv = CSV(fn, has_header=True)
        append(csv, data, dshape=ds)

        f = gzip.open(fn)
        s = f.read()
        assert 'name' in s
        assert 'Alice,1' in s
        f.close()


def test_pandas_loads_in_datetimes_naively():
    with filetext('name,when\nAlice,2014-01-01\nBob,2014-02-02') as fn:
        csv = CSV(fn, has_header=True)
        ds = datashape.dshape('var * {name: ?string, when: ?datetime}')
        assert discover(csv) == ds

        df = convert(pd.DataFrame, csv)
        assert df.dtypes['when'] == 'M8[ns]'


@skipif(os.name == 'nt')
def test_pandas_discover_on_gzipped_files():
    with filetext('name,when\nAlice,2014-01-01\nBob,2014-02-02',
                  open=gzip.open, mode='wt', extension='.csv.gz') as fn:
        csv = CSV(fn, has_header=True)
        ds = datashape.dshape('var * {name: ?string, when: ?datetime}')
        assert discover(csv) == ds


def test_csv_into_list():
    with filetext('name,val\nAlice,100\nBob,200', extension='csv') as fn:
        L = into(list, fn)
        assert L == [('Alice', 100), ('Bob', 200)]


def test_discover_csv_files_without_header():
    with filetext('Alice,2014-01-01\nBob,2014-02-02') as fn:
        csv = CSV(fn, has_header=False)
        df = convert(pd.DataFrame, csv)
        assert len(df) == 2
        assert 'Alice' not in list(df.columns)


def test_discover_csv_yields_string_on_totally_empty_columns():
    expected = dshape('var * {a: int64, b: ?string, c: int64}')
    with filetext('a,b,c\n1,,3\n4,,6\n7,,9') as fn:
        csv = CSV(fn, has_header=True)
        assert discover(csv) == expected


def test_glob():
    d = {'accounts1.csv': 'name,when\nAlice,100\nBob,200',
         'accounts2.csv': 'name,when\nAlice,300\nBob,400'}
    with filetexts(d) as fns:
        r = resource('accounts*.csv', has_header=True)
        assert convert(list, r) == [('Alice', 100), ('Bob', 200),
                                    ('Alice', 300), ('Bob', 400)]

        r = resource('*.csv')
        assert isinstance(r, chunks(CSV))


def test_pandas_csv_naive_behavior_results_in_columns():
    df = pd.DataFrame([[1, 'Alice',   100],
                       [2, 'Bob',    -200],
                       [3, 'Charlie', 300],
                       [4, 'Denis',   400],
                       [5, 'Edith',  -500]], columns=['id', 'name', 'amount'])
    with tmpfile('.csv') as fn:
        into(fn, df)

        with open(fn) as f:
            assert next(f).strip() == 'id,name,amount'


def test_discover_csv_without_columns():
    with filetext('Alice,100\nBob,200', extension='csv') as fn:
        csv = CSV(fn)
        ds = discover(csv)
        assert '100' not in str(ds)


def test_header_argument_set_with_or_without_header():
    with filetext('name,val\nAlice,100\nBob,200', extension='csv') as fn:
        assert into(list, fn) == [('Alice', 100), ('Bob', 200)]

    with filetext('Alice,100\nBob,200', extension='csv') as fn:
        assert into(list, fn) == [('Alice', 100), ('Bob', 200)]


def test_first_csv_establishes_consistent_dshape():
    d = {'accounts1.csv': 'name,when\nAlice,one\nBob,two',
         'accounts2.csv': 'name,when\nAlice,300\nBob,400'}
    with filetexts(d) as fns:
        L = into(list, 'accounts*.csv')
        assert len(L) == 4
        assert all(isinstance(val, (str, unicode)) for name, val in L)


def test_discover_csv_with_spaces_in_header():
    with filetext(' name,  val\nAlice,100\nBob,200', extension='csv') as fn:
        ds = discover(CSV(fn, has_header=True))
        assert ds.measure.names == ['name', 'val']


def test_header_disagrees_with_dshape():
    ds = datashape.dshape('var * {name: string, bal: int64}')
    with filetext('name,val\nAlice,100\nBob,200', extension='csv') as fn:
        csv = CSV(fn, header=True)
        assert convert(list, csv) == [('Alice', 100), ('Bob', 200)]

        assert list(convert(pd.DataFrame, csv).columns) == ['name', 'val']
        assert list(convert(pd.DataFrame, csv, dshape=ds).columns) == ['name', 'bal']


def test_raise_errors_quickly_on_into_chunks_dataframe():
    with filetext('name,val\nAlice,100\nBob,foo', extension='csv') as fn:
        ds = datashape.dshape('var * {name: string, val: int}')
        csv = CSV(fn, header=True)
        assert raises(Exception,
                lambda: CSV_to_chunks_of_dataframes(csv, dshape=ds))


def test_unused_datetime_columns():
    ds = datashape.dshape('var * {val: string, when: datetime}')
    with filetext("val,when\na,2000-01-01\nb,2000-02-02") as fn:
        csv = CSV(fn, has_header=True)
        assert convert(list, csv_to_DataFrame(csv, usecols=['val'],
            squeeze=True, dshape=ds)) == ['a', 'b']


def test_empty_dataframe():
    with filetext('name,val', extension='csv') as fn:
        csv = CSV(fn, has_header=True)
        df = convert(pd.DataFrame, csv)
        assert isinstance(df, pd.DataFrame)


def test_csv_missing_values():
    with filetext('name,val\nAlice,100\nNA,200', extension='csv') as fn:
        csv = CSV(fn)
        assert discover(csv).measure.dict['name'] == Option(string)


def test_csv_separator_header():
    with filetext('a|b|c\n1|2|3\n4|5|6', extension='csv') as fn:
        csv = CSV(fn, delimiter='|', has_header=True)
        assert convert(list, csv) == [(1, 2, 3), (4, 5, 6)]


df = pd.DataFrame([['Alice',   100],
                   ['Bob',     200],
                   ['Charlie', 300]],
                  columns=['name', 'balance'])


def test_temp_csv():
    csv = into(Temp(CSV)('_test_temp_csv.csv'), df)
    assert isinstance(csv, CSV)

    assert into(list, csv) == into(list, df)

    del csv
    import gc
    gc.collect()
    assert not os.path.exists('_test_temp_csv.csv')


def test_convert_to_csv():
    csv = into(Temp(CSV), df)
    assert isinstance(csv, CSV)

    assert into(list, csv) == into(list, df)
    assert isinstance(csv, _Temp)


@skipif(os.name == 'nt')
def test_unicode_column_names():
    with filetext('foo\xc4\x87,a\n1,2\n3,4', extension='csv') as fn:
        csv = CSV(fn, has_header=True)
        df = into(pd.DataFrame, csv)


def test_infer_header():
    with filetext('name,val\nAlice,100\nNA,200', extension='csv') as fn:
        assert infer_header(CSV(fn)) == True
    with filetext('Alice,100\nNA,200', extension='csv') as fn:
        assert infer_header(CSV(fn)) == False


def test_csv_supports_sep():
    assert CSV('foo.csv', sep=';').dialect['delimiter'] == ';'
