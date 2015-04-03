from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('sqlalchemy')

import os
import numpy as np
import pandas as pd
import sqlalchemy as sa
from datashape import discover, dshape
import datashape
from odo.backends.sql import (dshape_to_table, create_from_datashape,
                              dshape_to_alchemy)
from odo.utils import tmpfile, raises
from odo import convert, append, resource, discover, into, odo


def test_resource():
    sql = resource('sqlite:///:memory:::mytable',
                   dshape='var * {x: int, y: int}')
    assert isinstance(sql, sa.Table)
    assert sql.name == 'mytable'
    assert isinstance(sql.bind, sa.engine.base.Engine)
    assert set([c.name for c in sql.c]) == set(['x', 'y'])


def test_append_and_convert_round_trip():
    engine = sa.create_engine('sqlite:///:memory:')
    metadata = sa.MetaData(engine)
    t = sa.Table('bank', metadata,
                 sa.Column('name', sa.String, primary_key=True),
                 sa.Column('balance', sa.Integer))
    t.create()

    data = [('Alice', 1), ('Bob', 2)]
    append(t, data)

    assert convert(list, t) == data


def test_plus_must_have_text():
    with pytest.raises(NotImplementedError):
        resource('redshift+://user:pass@host:1234/db')


def test_resource_on_file():
    with tmpfile('.db') as fn:
        uri = 'sqlite:///' + fn
        sql = resource(uri, 'foo', dshape='var * {x: int, y: int}')
        assert isinstance(sql, sa.Table)

    with tmpfile('.db') as fn:
        uri = 'sqlite:///' + fn
        sql = resource(uri + '::' + 'foo', dshape='var * {x: int, y: int}')
        assert isinstance(sql, sa.Table)


def test_resource_to_engine():
    with tmpfile('.db') as fn:
        uri = 'sqlite:///' + fn
        r = resource(uri)
        assert isinstance(r, sa.engine.Engine)
        assert r.dialect.name == 'sqlite'


def test_resource_to_engine_to_create_tables():
    with tmpfile('.db') as fn:
        uri = 'sqlite:///' + fn
        ds = datashape.dshape('{mytable: var * {name: string, amt: int}}')
        r = resource(uri, dshape=ds)
        assert isinstance(r, sa.engine.Engine)
        assert r.dialect.name == 'sqlite'

        assert discover(r) == ds


def test_discover():
    assert discover(sa.String()) == datashape.string
    metadata = sa.MetaData()
    s = sa.Table('accounts', metadata,
                 sa.Column('name', sa.String),
                 sa.Column('amount', sa.Integer),
                 sa.Column('timestamp', sa.DateTime, primary_key=True))

    assert discover(s) == \
        dshape('var * {name: ?string, amount: ?int32, timestamp: datetime}')


def test_discover_numeric_column():
    assert discover(sa.String()) == datashape.string
    metadata = sa.MetaData()
    s = sa.Table('name', metadata,
                 sa.Column('name', sa.types.NUMERIC),)

    assert discover(s)


def test_discover_null_columns():
    assert dshape(discover(sa.Column('name', sa.String, nullable=True))) == \
        dshape('{name: ?string}')
    assert dshape(discover(sa.Column('name', sa.String, nullable=False))) == \
        dshape('{name: string}')


def test_discover_selectable():
    t = resource('sqlite:///:memory:::mytable',
                 dshape='var * {x: int, y: int}')
    q = sa.select([t.c.x]).limit(5)
    assert discover(q) == dshape('var * {x: int}')


def single_table_engine():
    engine = sa.create_engine('sqlite:///:memory:')
    metadata = sa.MetaData(engine)
    t = sa.Table('accounts', metadata,
                 sa.Column('name', sa.String),
                 sa.Column('amount', sa.Integer))
    t.create()
    return engine, t


def test_select_to_iterator():
    engine, t = single_table_engine()
    append(t, [('Alice', 100), ('Bob', 200)])

    sel = sa.select([t.c.amount + 1])

    assert convert(list, sel) == [(101,), (201,)]
    assert convert(list, sel, dshape=dshape('var * int')) == [101, 201]

    sel2 = sa.select([sa.sql.func.sum(t.c.amount)])

    assert convert(int, sel2, dshape=dshape('int')) == 300

    sel3 = sa.select([t])

    result = convert(list, sel3, dshape=discover(t))
    assert type(result[0]) is tuple


def test_discovery_engine():
    engine, t = single_table_engine()

    assert discover(engine, 'accounts') == discover(t)

    assert str(discover(engine)) == str(discover({'accounts': t}))


def test_discovery_metadata():
    engine, t = single_table_engine()
    metadata = t.metadata
    assert str(discover(metadata)) == str(discover({'accounts': t}))


def test_discover_views():
    engine, t = single_table_engine()
    metadata = t.metadata
    with engine.connect() as conn:
        conn.execute('''CREATE VIEW myview AS
                        SELECT name, amount
                        FROM accounts
                        WHERE amount > 0''')

    assert str(discover(metadata)) == str(
        discover({'accounts': t, 'myview': t}))


def test_extend_empty():
    engine, t = single_table_engine()

    assert not convert(list, t)
    append(t, [])
    assert not convert(list, t)


def test_dshape_to_alchemy():
    assert dshape_to_alchemy('string') == sa.Text
    assert isinstance(dshape_to_alchemy('string[40]'), sa.String)
    assert not isinstance(dshape_to_alchemy('string["ascii"]'), sa.Unicode)
    assert isinstance(dshape_to_alchemy('string[40, "U8"]'), sa.Unicode)
    assert dshape_to_alchemy('string[40]').length == 40

    assert dshape_to_alchemy('float32').precision == 24
    assert dshape_to_alchemy('float64').precision == 53


def test_dshape_to_table():
    t = dshape_to_table('bank', '{name: string, amount: int}')
    assert isinstance(t, sa.Table)
    assert t.name == 'bank'
    assert [c.name for c in t.c] == ['name', 'amount']


td_freqs = list(zip(['D', 'h', 'm', 's', 'ms', 'us', 'ns'],
                    [0, 0, 0, 0, 3, 6, 9],
                    [9, 0, 0, 0, 0, 0, 0]))


@pytest.mark.parametrize(['freq', 'secp', 'dayp'], td_freqs)
def test_dshape_to_table_with_timedelta(freq, secp, dayp):
    ds = '{name: string, amount: int, duration: timedelta[unit="%s"]}' % freq
    t = dshape_to_table('td_bank', ds)
    assert isinstance(t, sa.Table)
    assert t.name == 'td_bank'
    assert isinstance(t.c.duration.type, sa.types.Interval)
    assert t.c.duration.type.second_precision == secp
    assert t.c.duration.type.day_precision == dayp


@pytest.mark.xfail(raises=NotImplementedError)
def test_dshape_to_table_month():
    ds = '{name: string, amount: int, duration: timedelta[unit="M"]}'
    dshape_to_table('td_bank', ds)


@pytest.mark.xfail(raises=NotImplementedError)
def test_dshape_to_table_year():
    ds = '{name: string, amount: int, duration: timedelta[unit="Y"]}'
    dshape_to_table('td_bank', ds)


@pytest.mark.parametrize('freq', ['D', 's', 'ms', 'us', 'ns'])
def test_timedelta_sql_discovery(freq):
    ds = '{name: string, amount: int, duration: timedelta[unit="%s"]}' % freq
    t = dshape_to_table('td_bank', ds)
    assert discover(t).measure['duration'] == datashape.TimeDelta(freq)


@pytest.mark.parametrize('freq', ['h', 'm'])
def test_timedelta_sql_discovery_hour_minute(freq):
    # these always compare equal to a seconds timedelta, because no data loss
    # will occur with this heuristic. this implies that the sa.Table was
    # constructed with day_precision == 0 and second_precision == 0
    ds = '{name: string, amount: int, duration: timedelta[unit="%s"]}' % freq
    t = dshape_to_table('td_bank', ds)
    assert discover(t).measure['duration'] == datashape.TimeDelta('s')


prec = {
    's': 0,
    'ms': 3,
    'us': 6,
    'ns': 9
}


@pytest.mark.parametrize('freq', list(prec.keys()))
def test_discover_postgres_intervals(freq):
    precision = prec.get(freq)
    typ = sa.dialects.postgresql.base.INTERVAL(precision=precision)
    t = sa.Table('t', sa.MetaData(), sa.Column('dur', typ))
    assert discover(t) == dshape('var * {dur: ?timedelta[unit="%s"]}' % freq)


# between postgresql and oracle, only oracle has support for day intervals

@pytest.mark.parametrize('freq', ['D'] + list(prec.keys()))
def test_discover_oracle_intervals(freq):
    typ = sa.dialects.oracle.base.INTERVAL(day_precision={'D': 9}.get(freq),
                                           second_precision=prec.get(freq, 0))
    t = sa.Table('t', sa.MetaData(), sa.Column('dur', typ))
    assert discover(t) == dshape('var * {dur: ?timedelta[unit="%s"]}' % freq)


def test_create_from_datashape():
    engine = sa.create_engine('sqlite:///:memory:')
    ds = dshape('''{bank: var * {name: string, amount: int},
                    points: var * {x: int, y: int}}''')
    engine = create_from_datashape(engine, ds)

    assert discover(engine) == ds


def test_into_table_iterator():
    engine = sa.create_engine('sqlite:///:memory:')
    metadata = sa.MetaData(engine)
    t = dshape_to_table('points', '{x: int, y: int}', metadata=metadata)
    t.create()

    data = [(1, 1), (2, 4), (3, 9)]
    append(t, data)

    assert convert(list, t) == data
    assert isinstance(convert(list, t)[0], tuple)

    t2 = dshape_to_table('points2', '{x: int, y: int}', metadata=metadata)
    t2.create()
    data2 = [{'x': 1, 'y': 1}, {'x': 2, 'y': 4}, {'x': 3, 'y': 9}]
    append(t2, data2)

    assert convert(list, t2) == data


def test_sql_field_names_disagree_on_order():
    r = resource('sqlite:///:memory:::tb', dshape=dshape('{x: int, y: int}'))
    append(r, [(1, 2), (10, 20)], dshape=dshape('{y: int, x: int}'))
    assert convert(set, r) == set([(2, 1), (20, 10)])


def test_sql_field_names_disagree_on_names():
    r = resource('sqlite:///:memory:::tb', dshape=dshape('{x: int, y: int}'))
    assert raises(Exception, lambda: append(r, [(1, 2), (10, 20)],
                                            dshape=dshape('{x: int, z: int}')))


def test_resource_on_dialects():
    assert (resource.dispatch('mysql://foo') is
            resource.dispatch('mysql+pymysql://foo'))
    assert (resource.dispatch('never-before-seen-sql://foo') is
            resource.dispatch('mysql://foo'))


@pytest.yield_fixture
def sqlite_file():
    try:
        yield 'sqlite:///db.db'
    finally:
        os.remove('db.db')


def test_append_from_select(sqlite_file):
    # we can't test in memory here because that creates two independent
    # databases
    raw = np.array([(200.0, 'Glenn'),
                    (314.14, 'Hope'),
                    (235.43, 'Bob')], dtype=[('amount', 'float64'),
                                             ('name', 'S5')])
    raw2 = np.array([(800.0, 'Joe'),
                     (914.14, 'Alice'),
                     (1235.43, 'Ratso')], dtype=[('amount', 'float64'),
                                                 ('name', 'S5')])
    t = into('%s::t' % sqlite_file, raw)
    s = into('%s::s' % sqlite_file, raw2)
    t = append(t, s.select())
    result = into(list, t)
    expected = np.concatenate((raw, raw2)).tolist()
    assert result == expected


def test_append_from_table():
    # we can't test in memory here because that creates two independent
    # databases
    with tmpfile('db') as fn:
        raw = np.array([(200.0, 'Glenn'),
                        (314.14, 'Hope'),
                        (235.43, 'Bob')], dtype=[('amount', 'float64'),
                                                 ('name', 'S5')])
        raw2 = np.array([(800.0, 'Joe'),
                         (914.14, 'Alice'),
                         (1235.43, 'Ratso')], dtype=[('amount', 'float64'),
                                                     ('name', 'S5')])
        t = into('sqlite:///%s::t' % fn, raw)
        s = into('sqlite:///%s::s' % fn, raw2)
        t = append(t, s)
        result = odo(t, list)
        expected = np.concatenate((raw, raw2)).tolist()
        assert result == expected


def test_engine_metadata_caching():
    with tmpfile('db') as fn:
        engine = resource('sqlite:///' + fn)
        a = resource(
            'sqlite:///' + fn + '::a', dshape=dshape('var * {x: int}'))
        b = resource(
            'sqlite:///' + fn + '::b', dshape=dshape('var * {y: int}'))

        assert a.metadata is b.metadata
        assert engine is a.bind is b.bind


def test_copy_one_table_to_a_foreign_engine():
    data = [(1, 1), (2, 4), (3, 9)]
    ds = dshape('var * {x: int, y: int}')
    with tmpfile('db') as fn1:
        with tmpfile('db') as fn2:
            src = into('sqlite:///%s::points' % fn1, data, dshape=ds)
            tgt = into('sqlite:///%s::points' % fn2 + '::points',
                       sa.select([src]), dshape=ds)

            assert into(set, src) == into(set, tgt)
            assert into(set, data) == into(set, tgt)


def test_select_to_series_retains_name():
    data = [(1, 1), (2, 4), (3, 9)]
    ds = dshape('var * {x: int, y: int}')
    with tmpfile('db') as fn1:
        points = odo(data, 'sqlite:///%s::points' % fn1, dshape=ds)
        sel = sa.select([(points.c.x + 1).label('x')])
        series = odo(sel, pd.Series)
    assert series.name == 'x'
    assert odo(series, list) == [x + 1 for x, _ in data]


def test_empty_select_to_empty_frame():
    # data = [(1, 1), (2, 4), (3, 9)]
    ds = dshape('var * {x: int, y: int}')
    with tmpfile('db') as fn1:
        points = resource('sqlite:///%s::points' % fn1, dshape=ds)
        sel = sa.select([points])
        df = odo(sel, pd.DataFrame)
    assert df.empty
    assert df.columns.tolist() == ['x', 'y']
