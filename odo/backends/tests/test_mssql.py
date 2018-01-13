from __future__ import absolute_import, division, print_function

import pytest

sa = pytest.importorskip('sqlalchemy')
pymssql = pytest.importorskip('pymssql')

import datetime
import os
import itertools
import shutil

from datashape import dshape
import numpy as np
import pandas as pd

from odo.backends.csv import CSV
from odo.backends.sql import select_to_base
from odo import odo, into, resource, drop, discover, convert
from odo.utils import assert_allclose, tmpfile

try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus


names = ('tbl%d' % i for i in itertools.count())
new_schema = object()
data = [(1, 2), (10, 20), (100, 200)]
null_data = [(1, None), (10, 20), (100, 200)]


skip_no_rw_loc = \
    pytest.mark.skipif(1 == 0,
                       reason=("Skipping tests with remote PG server (%s)"))


@pytest.fixture(scope='module')
def tmpdir():
    return os.environ.get('MSSQL_TMP_DIR', None)


@pytest.fixture(scope='module')
def mssql_ip():
    return os.environ.get('MSSQL_IP', 'localhost')


@pytest.fixture(scope='module')
def mssql_dbname():
    return os.environ.get('MSSQL_DB', 'test')


@pytest.yield_fixture
def csv(tmpdir):
    s = '\r\n'.join(','.join(map(str, row)) for row in data).encode('utf-8')
    with tmpfile('.csv', dir=tmpdir) as fn:
        with open(fn, 'wb') as f:
            f.write(s)
        yield CSV(fn)


@pytest.yield_fixture
def encoding_csv(tmpdir):
    path = os.path.join(os.path.dirname(__file__), 'encoding.csv')
    with tmpfile('.csv', dir=tmpdir) as fn:
        with open(fn, 'wb') as f, open(path, 'r') as g:
            f.write(g.read().encode('latin1'))
        yield CSV(fn, columns=list('ab'))


@pytest.yield_fixture
def complex_csv(tmpdir):
    path = os.path.join(os.path.dirname(__file__), 'dummydata.csv')
    with tmpfile('.csv', dir=tmpdir) as fn:
        shutil.copy(path, fn)
        os.chmod(fn, 0o777)
        yield CSV(fn, has_header=True)


@pytest.fixture
def url():
    return 'mssql+pymssql://{ip}/{db}::{tbl}'.format(ip=mssql_ip(),
                                                     db=mssql_dbname(),
                                                     tbl=next(names))


def sql_fixture(dshape, schema=None):
    @pytest.yield_fixture(params=(True, False))
    def fixture(request, url):
        try:
            t = resource(
                url,
                dshape=dshape,
                schema=next(names) if schema is new_schema else schema,
            )
        except sa.exc.OperationalError as e:
            pytest.skip(str(e))
        else:
            try:
                bind = t.bind
                if not request.param:
                    t.metadata.bind = None
                yield t, bind
            finally:
                t.metadata.bind = bind
                drop(t)
                if t.schema:
                    bind.execute(sa.sql.ddl.DropSchema(t.schema))

    return fixture


sql = sql_fixture('var * {a: int32, b: ?int32}')
sql_with_schema = sql_fixture('var * {a: int32, b: ?int32}', new_schema)
sql_with_ugly_schema = sql_fixture('var * {a: int32, b: ?int32}', 'foo.b.ar')
complex_sql = sql_fixture("""
    var * {
        Name: string, RegistrationDate: date, ZipCode: int32, Consts: float64
    }
""")
sql_with_floats = sql_fixture('var * {a: float64, b: ?float64}')
sql_with_dts = sql_fixture('var * {a: datetime, b: ?datetime}')
sql_with_datelikes = sql_fixture("""
    var * {
        date: date,
        option_date: ?date,
        datetime: datetime,
        option_datetime: ?datetime,
    }
""")
sql_with_strings = sql_fixture("""
    var * {non_optional: string, optional: ?string}
""")

sql_with_bit = sql_fixture("""
    var * {bit1: bool, bit2: ?bool}
""")


@pytest.yield_fixture
def quoted_sql(mssql_ip, csv):
    url = 'mssql+pymssql://{}/test::foo bar'.format(mssql_ip)
    try:
        t = resource(url, dshape=discover(csv))
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            yield t
        finally:
            drop(t)


@skip_no_rw_loc
def test_simple_into(csv, sql):
    sql, bind = sql
    into(sql, csv, dshape=discover(sql), bind=bind)
    assert into(list, sql, bind=bind) == data


@skip_no_rw_loc
def test_append(csv, sql):
    sql, bind = sql
    into(sql, csv, bind=bind)
    assert into(list, sql, bind=bind) == data

    into(sql, csv, bind=bind)
    assert into(list, sql, bind=bind) == data + data


@pytest.mark.xfail(reason="bcp.exe doesn't mind weird line terminators")
def test_tryexcept_into(csv, sql):
    sql, bind = sql
    with pytest.raises(sa.exc.ProgrammingError):
        # uses multi-byte character
        into(sql, csv, rowterminator="alpha", bind=bind)


@skip_no_rw_loc
def test_no_header_no_columns(csv, sql):
    sql, bind = sql
    into(sql, csv, bind=bind, dshape=discover(sql))
    assert into(list, sql, bind=bind) == data


@skip_no_rw_loc
def test_complex_into(complex_csv, complex_sql):
    complex_sql, bind = complex_sql
    # data from: http://dummydata.me/generate
    into(complex_sql, complex_csv, dshape=discover(complex_sql), bind=bind)
    assert_allclose(
        into(list, complex_sql, bind=bind), into(list, complex_csv)
    )


@skip_no_rw_loc
def test_sql_to_csv(sql, csv, tmpdir):
    sql, bind = sql
    sql = odo(csv, sql, bind=bind)
    with tmpfile('.csv', dir=tmpdir) as fn:
        csv = odo(sql, fn, bind=bind)
        assert odo(csv, list) == data
        assert discover(csv).measure.names == discover(sql).measure.names


@skip_no_rw_loc
def test_sql_select_to_csv(sql, csv, tmpdir):
    sql, bind = sql
    sql = odo(csv, sql, bind=bind)
    query = sa.select([sql.c.a])
    with tmpfile('.csv', dir=tmpdir) as fn:
        csv = odo(query, fn, bind=bind)
        assert odo(csv, list) == [(x,) for x, _ in data]


@pytest.mark.xfail(reason="bcp.exe doesn't do escapechar")
def test_invalid_escapechar(sql, csv):
    sql, bind = sql
    with pytest.raises(ValueError):
        odo(csv, sql, escapechar='12', bind=bind)

    with pytest.raises(ValueError):
        odo(csv, sql, escapechar='', bind=bind)


@skip_no_rw_loc
def test_csv_output_is_not_quoted_by_default(sql, csv, tmpdir):
    sql, bind = sql
    sql = odo(csv, sql, bind=bind)
    expected = "a,b\n1,2\n10,20\n100,200\n"
    with tmpfile('.csv', dir=tmpdir) as fn:
        csv = odo(sql, fn, bind=bind, dshape=discover(sql))
        with open(fn, 'rt') as f:
            result = f.read()
        assert result == expected


@skip_no_rw_loc
def test_na_value(sql, csv, tmpdir):
    sql, bind = sql
    sql = odo(null_data, sql, bind=bind)
    with tmpfile('.csv', dir=tmpdir) as fn:
        csv = odo(sql, fn, na_value='NA', bind=bind)
        with open(csv.path, 'rt') as f:
            raw = f.read()
    assert raw == 'a,b\n1,\n10,20\n100,200\n'


@pytest.mark.xfail(reason="MSSQL uses code pages. not sure how to write this.")
def test_different_encoding(url, encoding_csv):
    try:
        sql = odo(encoding_csv, url, encoding='latin1')
    except sa.exc.OperationalError as e:
        pytest.skip(str(e))
    else:
        try:
            result = odo(sql, list)
            expected = [(u'1958.001.500131-1A', 1, None, u'', 899),
                        (u'1958.001.500156-6', 1, None, u'', 899),
                        (u'1958.001.500162-1', 1, None, u'', 899),
                        (u'1958.001.500204-2', 1, None, u'', 899),
                        (u'1958.001.500204-2A', 1, None, u'', 899),
                        (u'1958.001.500204-2B', 1, None, u'', 899),
                        (u'1958.001.500223-6', 1, None, u'', 9610),
                        (u'1958.001.500233-9', 1, None, u'', 4703),
                        (u'1909.017.000018-3', 1, 30.0, u'sumaria', 899)]
            assert result == expected
        finally:
            drop(sql)


@skip_no_rw_loc
def test_schema(csv, sql_with_schema):
    sql_with_schema, bind = sql_with_schema
    assert odo(odo(csv, sql_with_schema, bind=bind), list, bind=bind) == data


@pytest.mark.xfail(reason="MSSQL thinks foo is a database")
@skip_no_rw_loc
def test_ugly_schema(csv, sql_with_ugly_schema):
    sql_with_ugly_schema, bind = sql_with_ugly_schema
    assert (
        odo(odo(csv, sql_with_ugly_schema, bind=bind), list, bind=bind) ==
        data
    )


def test_schema_discover(sql_with_schema):
    sql_with_schema, bind = sql_with_schema
    sql_with_schema.metadata.bind = bind
    meta = discover(sql_with_schema.metadata)
    assert meta == dshape('{%s: var * {a: int32, b: ?int32}}' %
                          sql_with_schema.name)


@skip_no_rw_loc
def test_quoted_name(quoted_sql, csv):
    s = odo(csv, quoted_sql)
    t = odo(csv, list)
    assert sorted(odo(s, list)) == sorted(t)


def test_path_of_reduction(sql):
    sql, bind = sql
    result = list(convert.path(sa.select([sa.func.sum(sql.c.a)]), float))
    expected = [(sa.sql.Select, float, select_to_base, 200)]
    assert result == expected


def test_drop_reflects_database_state(url):
    data = list(zip(range(5), range(1, 6)))

    t = odo(data, url, dshape='var * {A: int64, B: int64}')
    assert t.exists()
    assert resource(url).exists()

    drop(url)
    with pytest.raises(ValueError):
        resource(url)  # Table doesn't exist and no dshape


@pytest.mark.xfail(reason="MSSQL doens't really do nan's either")
def test_nan_stays_nan(sql_with_floats):
    sql_with_floats, bind = sql_with_floats

    df = pd.DataFrame({'a': [0., 1., 2.], 'b': [3, np.nan, 4]})
    odo(df, sql_with_floats, bind=bind)
    rehydrated = odo(sql_with_floats, pd.DataFrame, bind=bind)
    pd.util.testing.assert_frame_equal(rehydrated.sort_index(axis=1), df)

    nulls_query = sa.select(sql_with_floats.c).where(
        sql_with_floats.c.a.is_(None) | sql_with_floats.c.b.is_(None)
    )
    if bind is None:
        nulls = nulls_query.execute()
    else:
        nulls = bind.execute(nulls_query)
    assert not nulls.fetchall()


def test_nat_to_null(sql_with_dts):
    sql_with_dts, bind = sql_with_dts

    df = pd.DataFrame({'a': pd.to_datetime(['2014-01-01', '2014-01-02']),
                       'b': pd.to_datetime(['2014-01-01', 'nat'])})
    odo(df, sql_with_dts, bind=bind)
    rehydrated = odo(sql_with_dts, pd.DataFrame, bind=bind)
    pd.util.testing.assert_frame_equal(rehydrated.sort_index(axis=1), df)

    nulls_query = sa.select(sql_with_dts.c).where(
        sql_with_dts.c.a.is_(None) | sql_with_dts.c.b.is_(None)
    )
    if bind is None:
        nulls = nulls_query.execute()
    else:
        nulls = bind.execute(nulls_query)
    assert nulls.fetchall() == [(pd.Timestamp('2014-01-02'), None)]


def test_to_dataframe(sql):
    sql, bind = sql
    insert_query = sql.insert().values([{'a': 1, 'b': 2}, {'a': 3, 'b': 4}])
    if bind is None:
        insert_query.execute()
    else:
        bind.execute(insert_query)

    df = odo(sql, pd.DataFrame, bind=bind)
    expected = pd.DataFrame(np.array([(1, 2), (3, 4)],
                                     dtype=[('a', 'int32'), ('b', 'float64')]))
    pd.util.testing.assert_frame_equal(df, expected)


def test_to_dataframe_bit(sql_with_bit):
    sql, bind = sql_with_bit
    insert_query = sql.insert().values([
        {'bit1': 1, 'bit2': None},
        {'bit1': 0, 'bit2': 1},
        {'bit1': 1, 'bit2': 0}
    ])
    if bind is None:
        insert_query.execute()
    else:
        bind.execute(insert_query)
    df = odo(sql, pd.DataFrame, bind=bind)
    expected = pd.DataFrame({'bit1': [True, False, True],
                             'bit2': [False, True, False]})
    assert df.bit1.dtype == 'bool'
    assert df.bit2.dtype == 'bool'
    pd.util.testing.assert_frame_equal(df, expected)

    ndarr = odo(sql, np.ndarray, bind=bind)
    assert ndarr.bit1.dtype == 'bool'
    assert ndarr.bit2.dtype == 'bool'


def test_to_dataframe_money():
    from sqlalchemy import create_engine
    import sqlalchemy as sa
    tbl_name = next(names)
    url = 'mssql+pymssql://{ip}/{db}'.format(ip=mssql_ip(),
                                             db=mssql_dbname())
    bind = create_engine(url)
    metadata = sa.MetaData()
    t = sa.Table(tbl_name, metadata,
                 sa.Column('money1', sa.dialects.mssql.MONEY, nullable=False),
                 sa.Column('money2', sa.dialects.mssql.MONEY, nullable=True))
    metadata.create_all(bind)
    insert_query = t.insert().values([
        {'money1': 100.1, 'money2': None},
        {'money1': 200.2, 'money2': 50.5},
        {'money1': 300.3, 'money2': 100.2}
    ])
    bind.execute(insert_query)
    sql = resource('{}::{}'.format(url, tbl_name))
    df = odo(sql, pd.DataFrame, bind=bind)
    expected = pd.DataFrame({'money1': [100.1, 200.2, 300.3],
                             'money2': [np.nan, 50.5, 100.2]})
    assert df.money1.dtype == 'float'
    assert df.money2.dtype == 'float'
    pd.util.testing.assert_frame_equal(df, expected)

    ndarr = odo(sql, np.ndarray, bind=bind)
    assert ndarr.money1.dtype == 'float'
    assert ndarr.money2.dtype == 'float'
    sql.drop()


def test_to_dataframe_datelike(sql_with_datelikes):
    sql_with_datelikes, bind = sql_with_datelikes
    date_0 = datetime.date(2014, 1, 1)
    date_1 = datetime.date(2014, 1, 2)
    datetime_0 = datetime.datetime(2014, 1, 1)
    datetime_1 = datetime.datetime(2014, 1, 2)
    insert_query = sql_with_datelikes.insert().values([
        {'date': date_0,
         'option_date': date_0,
         'datetime': datetime_0,
         'option_datetime': datetime_0},
        {'date': date_1,
         'option_date': date_1,
         'datetime': datetime_1,
         'option_datetime': datetime_1},
    ])
    if bind is None:
        insert_query.execute()
    else:
        bind.execute(insert_query)

    df = odo(sql_with_datelikes, pd.DataFrame, bind=bind)
    expected = pd.DataFrame([[datetime_0] * 4, [datetime_1] * 4],
                            columns=['date',
                                     'option_date',
                                     'datetime',
                                     'option_datetime'])
    pd.util.testing.assert_frame_equal(df, expected)


def test_to_dataframe_strings(sql_with_strings):
    sql_with_strings, bind = sql_with_strings

    insert_query = sql_with_strings.insert().values([
        {'non_optional': 'ayy', 'optional': 'hello "world"'},
        {'non_optional': 'lmao', 'optional': None},
    ])
    if bind is None:
        insert_query.execute()
    else:
        bind.execute(insert_query)

    df = odo(sql_with_strings, pd.DataFrame, bind=bind)
    expected = pd.DataFrame([['ayy', 'hello "world"'], ['lmao', None]],
                            columns=['non_optional', 'optional'])
    pd.util.testing.assert_frame_equal(df, expected)


def test_from_dataframe_strings(sql_with_strings):
    sql_with_strings, bind = sql_with_strings

    input_ = pd.DataFrame([['ayy', 'hello "world"'], ['lmao', None]],
                          columns=['non_optional', 'optional'])
    odo(input_, sql_with_strings, bind=bind)
    output = odo(sql_with_strings, pd.DataFrame, bind=bind)
    pd.util.testing.assert_frame_equal(output, input_)


def test_pipe_delimited(sql):
    sql, bind = sql
    expected = 'a|b\n1|\n10|200\n100|200\n'
    with tmpfile('.csv') as path, tmpfile('.csv') as csv:
        with open(path, 'w+') as f:
            f.write(expected)
        t = odo(path, sql, delimiter='|', bind=bind)
        odo(t, csv, delimiter='|', bind=bind)
        with open(csv, 'r') as f:
            assert f.read() == expected
