from __future__ import absolute_import, division, print_function

import pytest

import sys
import os
import numpy as np
import pandas as pd
import pandas.util.testing as tm
import gzip
import datashape
from datashape import Option, string
from collections import Iterator

from odo.backends.csv import (CSV, append, convert, resource,
                              csv_to_dataframe, CSV_to_chunks_of_dataframes,
                              infer_header)
from odo.utils import tmpfile, filetext, filetexts, raises
from odo import (into, append, convert, resource, discover, dshape, Temp,
                 chunks, odo)
from odo.temp import _Temp
from odo.compatibility import unicode


def test_csv():
    with tmpfile('.csv') as fn:
        csv = CSV(
            fn, dshape='var * {name: string, amount: int}', delimiter=',')

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
        df = csv_to_dataframe(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice', 1), ('Bob', 2)]
        assert list(df.columns) == ['name', 'amount']


def test_pandas_read_supports_datetimes():
    with filetext('Alice,2014-01-02\nBob,2014-01-03') as fn:
        ds = datashape.dshape('var * {name: string, when: date}')
        csv = CSV(fn)
        df = csv_to_dataframe(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['name', 'when']
        assert df.dtypes['when'] == 'M8[ns]'


def test_pandas_read_supports_whitespace_strings():
    with filetext('a,b, \n1,2, \n2,3, \n', extension='csv') as fn:
        csv = CSV(fn)
        ds = discover(csv)
        assert ds == datashape.dshape("var * {a: int64, b: int64, '': ?string}")


def test_pandas_read_supports_missing_integers():
    with filetext('Alice,1\nBob,') as fn:
        ds = datashape.dshape('var * {name: string, val: ?int32}')
        csv = CSV(fn)
        df = csv_to_dataframe(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == ['name', 'val']
        assert df.dtypes['val'] == 'f4'


@pytest.mark.xfail(sys.platform == 'win32' and sys.version_info[0] < 3,
                   reason="Doesn't work on Windows")
def test_pandas_read_supports_gzip():
    with filetext('Alice,1\nBob,2', open=gzip.open,
                  mode='wt', extension='.csv.gz') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_dataframe(csv, dshape=ds)
        assert isinstance(df, pd.DataFrame)
        assert convert(list, df) == [('Alice', 1), ('Bob', 2)]
        assert list(df.columns) == ['name', 'amount']


def test_pandas_read_supports_read_csv_kwargs():
    with filetext('Alice,1\nBob,2') as fn:
        ds = datashape.dshape('var * {name: string, amount: int}')
        csv = CSV(fn)
        df = csv_to_dataframe(csv, dshape=ds, usecols=['name'])
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


@pytest.mark.xfail(sys.version_info[0] == 3, reason="Doesn't work on Python 3")
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


@pytest.mark.xfail(sys.platform == 'win32' and sys.version_info[0] < 3,
                   reason="Doesn't work on Windows")
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
        result = into(list, 'accounts*.csv')
        assert len(result) == 4
        assert all(isinstance(val, (str, unicode)) for name, val in result)


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
        assert list(convert(pd.DataFrame, csv, dshape=ds).columns) == [
            'name', 'bal']


def test_header_mix_str_digits():
    ds = datashape.dshape('''var * {"On- or Off- Budget": ?string,
                                    "1990": ?string}''')
    with filetext('On- or Off- Budget,1990\nOn Budget,-628\nOff budget,"5,962"\n') as fn:
        csv = CSV(fn, has_header=True)
        df = convert(pd.DataFrame, csv)
        assert discover(csv).measure == ds.measure


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
        assert convert(list, csv_to_dataframe(csv, usecols=['val'],
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


def test_unicode_column_names():
    with filetext(b'f\xc3\xbc,a\n1,2\n3,4', extension='csv', mode='wb') as fn:
        df = into(pd.DataFrame, CSV(fn, has_header=True))
    expected = pd.DataFrame([(1, 2), (3, 4)],
                            columns=[b'f\xc3\xbc'.decode('utf8'), u'a'])
    tm.assert_frame_equal(df, expected)


def test_more_unicode_column_names():
    with filetext(b'foo\xc4\x87,a\n1,2\n3,4', extension='csv',
                  mode='wb') as fn:
        df = into(pd.DataFrame, CSV(fn, has_header=True))
    expected = pd.DataFrame([(1, 2), (3, 4)],
                            columns=[b'foo\xc4\x87'.decode('utf8'), u'a'])
    tm.assert_frame_equal(df, expected)


def test_infer_header():
    with filetext('name,val\nAlice,100\nNA,200', extension='csv') as fn:
        assert infer_header(CSV(fn).path, 100) == True
    with filetext('Alice,100\nNA,200', extension='csv') as fn:
        assert infer_header(CSV(fn).path, 100) == False


def test_csv_supports_sep():
    assert CSV('foo.csv', sep=';').dialect['delimiter'] == ';'


def test_csv_to_compressed_csv():
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write('a,1\nb,2\nc,3')
        with tmpfile('.csv.gz') as gfn:
            result = odo(fn, gfn)
            assert odo(result, list) == odo(fn, list)


def test_has_header_on_tsv():
    with tmpfile('.csv') as fn:
        with open(fn, 'wb') as f:
            f.write(b'a\tb\n1\t2\n3\t4')
        csv = CSV(fn)
        assert csv.has_header


def test_header_with_quotes():
    csv = CSV(os.path.join(os.path.dirname(__file__), 'encoding.csv'),
              encoding='latin1')
    expected = dshape("""var * {
        D_PROC: ?string,
        NUM_SEQ: int64,
        COD_TIP_RELAC: ?float64,
        COMPL: ?string,
        COD_ASSUNTO: int64
    }
    """)
    assert discover(csv) == expected


def test_encoding_is_none():
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write('a,1\nb,2\nc,3'.encode('utf-8').decode('utf-8'))
        assert CSV(fn, encoding=None).encoding == 'utf-8'


def test_discover_with_dotted_names():
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write('a.b,c.d\n1,2\n3,4')
        dshape = discover(resource(fn))
    assert dshape == datashape.dshape('var * {"a.b": int64, "c.d": int64}')
    assert dshape.measure.names == [u'a.b', u'c.d']


try:
    unichr
except NameError:
    unichr = chr


def random_multibyte_string(nrows, string_length,
                            domain=''.join(map(unichr, range(1488, 1515)))):
    """ Generate `n` strings of length `string_length` sampled from `domain`.

    Parameters
    ----------
    n : int
        Number of random strings to generate
    string_length : int
        Length of each random string
    domain : str, optional
        The set of characters to sample from. Defaults to Hebrew.
    """
    for _ in range(nrows):
        yield ''.join(np.random.choice(list(domain), size=string_length))


@pytest.yield_fixture
def multibyte_csv():
    header = random_multibyte_string(nrows=2, string_length=3)
    single_column = random_multibyte_string(nrows=10, string_length=4)
    numbers = np.random.randint(4, size=10)
    with tmpfile('.csv') as fn:
        with open(fn, 'wb') as f:
            f.write((','.join(header) + '\n').encode('utf8'))
            f.write('\n'.join(','.join(map(unicode, row))
                              for row in zip(single_column, numbers)).encode('utf8'))
        yield fn


def test_multibyte_encoding_header(multibyte_csv):
        c = CSV(multibyte_csv, encoding='utf8', sniff_nbytes=3)
        assert c.has_header is None  # not enough data to infer header


def test_multibyte_encoding_dialect(multibyte_csv):
        c = CSV(multibyte_csv, encoding='utf8', sniff_nbytes=10)
        assert c.dialect['delimiter'] == ','


@pytest.mark.parametrize('string_dshape', ['string', 'string[25]'])
def test_string_n_convert(string_dshape):
    data = [
        '2015-03-13,FOO THE BAR',
        '2014-01-29,BAZ THE QUUX'
    ]
    ds = 'var * {k: date, n: %s}' % string_dshape
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write('\n'.join(data))
        csv = CSV(fn, has_header=False)
        result = odo(csv, pd.DataFrame, dshape=ds)
        assert list(result.columns) == list('kn')
    raw = [tuple(x.split(',')) for x in data]
    expected = pd.DataFrame(raw, columns=list('kn'))
    expected['k'] = pd.to_datetime(expected.k)
    tm.assert_frame_equal(result, expected)


def test_globbed_csv_to_chunks_of_dataframe():
    header = 'a,b,c\n'
    d = {'a-1.csv': header + '1,2,3\n4,5,6\n',
         'a-2.csv': header + '7,8,9\n10,11,12\n'}

    with filetexts(d):
        dfs = list(odo('a-*.csv', chunks(pd.DataFrame)))

    assert len(dfs) == 2
    columns = 'a', 'b', 'c'
    tm.assert_frame_equal(dfs[0],
                          pd.DataFrame([[1, 2, 3], [4, 5, 6]], columns=columns))
    tm.assert_frame_equal(dfs[1],
                          pd.DataFrame([[7, 8, 9], [10, 11, 12]], columns=columns))


def test_globbed_csv_to_dataframe():
    header = 'a,b,c\n'
    d = {'a-1.csv': header + '1,2,3\n4,5,6\n',
         'a-2.csv': header + '7,8,9\n10,11,12\n'}

    with filetexts(d):
        df = odo('a-*.csv', pd.DataFrame)

    tm.assert_frame_equal(
        df,
        pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]],
                     columns=['a', 'b', 'c']),
    )
