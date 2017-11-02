from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('sqlalchemy')

import os
from decimal import Decimal
from functools import partial
from textwrap import dedent

import datashape
from datashape import (
    date_,
    datetime_,
    discover,
    dshape,
    int_,
    int64,
    float32,
    float64,
    string,
    var,
    Option,
    R,
)
from datashape.util.testing import assert_dshape_equal
import numpy as np
import pandas as pd
import sqlalchemy as sa

from odo import convert, append, resource, into, odo, chunks
from odo.backends.sql import (
    dshape_to_table, create_from_datashape, dshape_to_alchemy,
    discover_sqlalchemy_selectable
)
from odo.utils import tmpfile, raises

from six import string_types


def test_resource():
    sql = resource('sqlite:///:memory:::mytable',
                   dshape='var * {x: int, y: int}')
    assert isinstance(sql, sa.Table)
    assert sql.name == 'mytable'
    assert isinstance(sql.bind, sa.engine.base.Engine)
    assert set(c.name for c in sql.c) == set(['x', 'y'])


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
    ds = dshape('var * {name: ?string, amount: ?int32, timestamp: datetime}')
    assert_dshape_equal(discover(s), ds)
    for name in ds.measure.names:
        assert isinstance(name, string_types)

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


def test_discover_fixed_length_string():
    t = resource('sqlite:///:memory:::mytable',
                 dshape='var * {x: string[30]}')
    assert discover(t) == dshape('var * {x: string[30]}')


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
    for res in result:
        assert isinstance(res[0], string_types)


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

    assert dshape_to_alchemy('float32') == sa.REAL
    assert dshape_to_alchemy('float64') == sa.FLOAT


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


@pytest.mark.parametrize(
    'typ,dtype', (
        (sa.DATETIME, datetime_),
        (sa.TIMESTAMP, datetime_),
        (sa.FLOAT, float64),
        (sa.DATE, date_),
        (sa.BIGINT, int64),
        (sa.INTEGER, int_),
        (sa.BIGINT, int64),
        (sa.types.NullType, string),
        (sa.REAL, float32),
        (sa.Float, float64),
        (sa.Float(precision=8), float32),
        (sa.Float(precision=24), float32),
        (sa.Float(precision=42), float64),
        (sa.Float(precision=53), float64),
    ),
)
def test_types(typ, dtype):
    expected = var * R['value': Option(dtype)]
    t = sa.Table('t', sa.MetaData(), sa.Column('value', typ))
    assert_dshape_equal(discover(t), expected)


@pytest.mark.parametrize(
    'typ', (
        sa.Float(precision=-1),
        sa.Float(precision=0),
        sa.Float(precision=54)
    )
)
def test_unsupported_precision(typ):
    t = sa.Table('t', sa.MetaData(), sa.Column('value', typ))
    with pytest.raises(ValueError) as err:
        discover(t)
    assert str(err.value) == "{} is not a supported precision".format(
        typ.precision)


def test_mssql_types():
    typ = sa.dialects.mssql.BIT()
    t = sa.Table('t', sa.MetaData(), sa.Column('bit', typ))
    assert_dshape_equal(discover(t), dshape('var * {bit: ?bool}'))
    typ = sa.dialects.mssql.DATETIMEOFFSET()
    t = sa.Table('t', sa.MetaData(), sa.Column('dt', typ))
    assert_dshape_equal(discover(t), dshape('var * {dt: ?string}'))
    typ = sa.dialects.mssql.MONEY()
    t = sa.Table('t', sa.MetaData(), sa.Column('money', typ))
    assert_dshape_equal(discover(t), dshape('var * {money: ?float64}'))
    typ = sa.dialects.mssql.SMALLMONEY()
    t = sa.Table('t', sa.MetaData(), sa.Column('money', typ))
    assert_dshape_equal(discover(t), dshape('var * {money: ?float32}'))
    typ = sa.dialects.mssql.UNIQUEIDENTIFIER()
    t = sa.Table('t', sa.MetaData(), sa.Column('uuid', typ))
    assert_dshape_equal(discover(t), dshape('var * {uuid: ?string}'))

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
                                             ('name', 'U5')])
    raw2 = np.array([(800.0, 'Joe'),
                     (914.14, 'Alice'),
                     (1235.43, 'Ratso')], dtype=[('amount', 'float64'),
                                                 ('name', 'U5')])
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
                                                 ('name', 'U5')])
        raw2 = np.array([(800.0, 'Joe'),
                         (914.14, 'Alice'),
                         (1235.43, 'Ratso')], dtype=[('amount', 'float64'),
                                                     ('name', 'U5')])
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
            tgt = into('sqlite:///%s::points' % fn2,
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


def test_discover_foreign_keys():
    with tmpfile('db') as fn:
        products = resource('sqlite:///%s::products' % fn,
                            dshape="""
                                var * {
                                    product_no: int32,
                                    name: ?string,
                                    price: ?float64
                                }
                            """,
                            primary_key=['product_no'])
        expected = dshape("""var * {
                          order_id: int32,
                          product_no: map[int32, {
                            product_no: int32,
                            name: ?string,
                            price: ?float64
                          }],
                          quantity: ?int32
                        }""")
        orders = resource('sqlite:///%s::orders' % fn,
                          dshape=expected,
                          foreign_keys=dict(product_no=products.c.product_no))
        result = discover(orders)
        assert result == expected


def test_invalid_foreign_keys():
    with tmpfile('db') as fn:
        expected = dshape("""var * {
                          order_id: int32,
                          product_no: map[int32, {
                            product_no: int32,
                            name: ?string,
                            price: ?float64
                          }],
                          quantity: ?int32
                        }""")
        with pytest.raises(TypeError):
            resource('sqlite:///%s::orders' % fn, dshape=expected)


def test_foreign_keys_auto_construct():
    with tmpfile('db') as fn:
        products = resource('sqlite:///%s::products' % fn,
                            dshape="""
                                var * {
                                    product_no: int32,
                                    name: ?string,
                                    price: ?float64
                                }
                            """,
                            primary_key=['product_no'])
        ds = dshape("""var * {
                          order_id: int32,
                          product_no: map[int32, T],
                          quantity: ?int32
                        }""")
        orders = resource('sqlite:///%s::orders' % fn, dshape=ds,
                          foreign_keys=dict(product_no=products.c.product_no),
                          primary_key=['order_id'])
        assert discover(orders) == dshape("""
            var * {
                order_id: int32,
                product_no: map[int32, {
                                    product_no: int32,
                                    name: ?string,
                                    price: ?float64
                                }],
                quantity: ?int32
            }
        """)


def test_foreign_keys_bad_field():
    with tmpfile('db') as fn:
        expected = dshape("""var * {
                          order_id: int32,
                          product_no: int64,
                          quantity: ?int32
                        }""")
        with pytest.raises(TypeError):
            resource('sqlite:///%s::orders' % fn, dshape=expected,
                     foreign_keys=dict(foo='products.product_no'))


@pytest.fixture
def recursive_fkey():
    return sa.Table(
        'employees',
        sa.MetaData(),
        sa.Column('eid', sa.BIGINT, primary_key=True),
        sa.Column('name', sa.TEXT),
        sa.Column('mgr_eid', sa.BIGINT, sa.ForeignKey('employees.eid'),
                  nullable=False)
    )


def test_recursive_foreign_key(recursive_fkey):
    expected = dshape("""
        var * {
            eid: int64,
            name: ?string,
            mgr_eid: map[int64, {eid: int64, name: ?string, mgr_eid: int64}]
        }
    """)
    assert discover(recursive_fkey) == expected


def test_create_recursive_foreign_key():
    with tmpfile('.db') as fn:
        t = resource('sqlite:///%s::employees' % fn,
                     dshape="""
                     var * {
                        eid: int64,
                        name: ?string,
                        mgr_eid: map[int64, T]
                     }""", foreign_keys=dict(mgr_eid='employees.eid'),
                     primary_key=['eid'])
        result = discover(t)
    expected = dshape("""
        var * {
            eid: int64,
            name: ?string,
            mgr_eid: map[int64, {eid: int64, name: ?string, mgr_eid: int64}]
        }
    """)
    assert result == expected


def test_compound_primary_key():
    with tmpfile('db') as fn:
        products = resource('sqlite:///%s::products' % fn,
                            dshape="""
                                var * {
                                    product_no: int32,
                                    product_sku: string,
                                    name: ?string,
                                    price: ?float64
                                }
                            """,
                            primary_key=['product_no', 'product_sku'])
        assert len(products.primary_key) == 2
        assert (products.primary_key.columns['product_no'] is
                products.c.product_no)
        assert (products.primary_key.columns['product_sku'] is
                products.c.product_sku)


def test_compound_primary_key_with_fkey():
    with tmpfile('db') as fn:
        products = resource('sqlite:///%s::products' % fn,
                            dshape="""
                                var * {
                                    product_no: int32,
                                    product_sku: string,
                                    name: ?string,
                                    price: ?float64
                                }
                            """,
                            primary_key=['product_no', 'product_sku'])
        ds = dshape("""var * {
                          order_id: int32,
                          product_no: map[int32, T],
                          product_sku: map[int32, U],
                          quantity: ?int32
                        }""")
        orders = resource('sqlite:///%s::orders' % fn, dshape=ds,
                          primary_key=['order_id'],
                          foreign_keys={
                              'product_no': products.c.product_no,
                              'product_sku': products.c.product_sku
                          })
        assert discover(orders) == dshape(
            """var * {
                order_id: int32,
                product_no: map[int32, {product_no: int32, product_sku: string, name: ?string, price: ?float64}],
                product_sku: map[int32, {product_no: int32, product_sku: string, name: ?string, price: ?float64}],
                quantity: ?int32
            }
            """
        )


def test_compound_primary_key_with_single_reference():
    with tmpfile('db') as fn:
        products = resource('sqlite:///%s::products' % fn,
                            dshape="""
                                var * {
                                    product_no: int32,
                                    product_sku: string,
                                    name: ?string,
                                    price: ?float64
                                }
                            """, primary_key=['product_no', 'product_sku'])
        # TODO: should this fail everywhere? e.g., this fails in postgres, but
        # not in sqlite because postgres doesn't allow partial foreign keys
        # might be best to let the backend handle this
        ds = dshape("""var * {
                          order_id: int32,
                          product_no: map[int32, T],
                          quantity: ?int32
                        }""")
        orders = resource('sqlite:///%s::orders' % fn, dshape=ds,
                          foreign_keys=dict(product_no=products.c.product_no),
                          primary_key=['order_id'])
        assert discover(orders) == dshape(
            """var * {
                order_id: int32,
                product_no: map[int32, {product_no: int32, product_sku: string, name: ?string, price: ?float64}],
                quantity: ?int32
            }
            """
        )


def test_foreign_keys_as_compound_primary_key():
    with tmpfile('db') as fn:
        suppliers = resource(
            'sqlite:///%s::suppliers' % fn,
            dshape='var * {id: int64, name: string}',
            primary_key=['id']
        )
        parts = resource(
            'sqlite:///%s::parts' % fn,
            dshape='var * {id: int64, name: string, region: string}',
            primary_key=['id']
        )
        suppart = resource(
            'sqlite:///%s::suppart' % fn,
            dshape='var * {supp_id: map[int64, T], part_id: map[int64, U]}',
            foreign_keys={
                'supp_id': suppliers.c.id,
                'part_id': parts.c.id
            },
            primary_key=['supp_id', 'part_id']
        )
        expected = dshape("""
            var * {
                supp_id: map[int64, {id: int64, name: string}],
                part_id: map[int64, {id: int64, name: string, region: string}]
            }
        """)
        result = discover(suppart)
        assert result == expected


def test_append_chunks():
    tbl = resource('sqlite:///:memory:::test', dshape='var * {a: int, b: int}')
    res = odo(
        chunks(np.ndarray)((
            np.array([[0, 1], [2, 3]]),
            np.array([[4, 5], [6, 7]]),
        )),
        tbl,
    )
    assert res is tbl
    assert (
        odo(tbl, np.ndarray) == np.array(
            [(0, 1),
             (2, 3),
             (4, 5),
             (6, 7)],
            dtype=[('a', '<i4'), ('b', '<i4')],
        )
    ).all()


def test_append_array_without_column_names():
    with pytest.raises(TypeError):
        odo(np.zeros((2, 2)), 'sqlite:///:memory:::test')


def test_numeric_create():
    tbl = resource(
        'sqlite:///:memory:::test',
        dshape='var * {a: ?decimal[11, 2], b: decimal[10, 6]}'
    )
    assert tbl.c.a.nullable
    assert not tbl.c.b.nullable
    assert isinstance(tbl.c.a.type, sa.NUMERIC)
    assert isinstance(tbl.c.b.type, sa.NUMERIC)


def test_numeric_append():
    tbl = resource(
        'sqlite:///:memory:::test',
        dshape='var * {a: decimal[11, 2], b: ?decimal[10, 6]}'
    )
    data = [(1.0, 2.0), (2.0, 3.0)]
    tbl = odo(data, tbl)
    assert odo(tbl, list) == list(map(
        lambda row: tuple(map(Decimal, row)),
        tbl.select().execute().fetchall()
    ))


def test_discover_float_and_real_core_types():
    assert discover(sa.FLOAT()) == float64
    assert discover(sa.REAL()) == float32


def test_string_dshape_doc_example():
    x = np.zeros((10, 2))
    with tmpfile('.db') as fn:
        t = odo(
            x, 'sqlite:///%s::x' % fn, dshape='var * {a: float64, b: float64}'
        )
        assert all(row == (0, 0) for row in t.select().execute().fetchall())


def test_decimal_conversion():
    data = [(1.0,), (2.0,)]
    with tmpfile('.db') as fn:
        t = odo(data, 'sqlite:///%s::x' % fn, dshape='var * {x: decimal[11, 2]}')
        result = odo(sa.select([sa.func.sum(t.c.x)]), Decimal)
    assert result == sum(Decimal(r[0]) for r in data)


def test_append_empty_iterator_returns_table():
    with tmpfile('.db') as fn:
        t = resource('sqlite:///%s::x' % fn, dshape='var * {a: int32}')
        assert odo(iter([]), t) is t


def test_pass_non_hashable_arg_to_create_engine():
    with tmpfile('.db') as fn:
        r = partial(
            resource,
            'sqlite:///%s::t' % fn,
            connect_args={},
            dshape='var * {a: int32}',
        )
        assert r() is r()

    s = partial(
        resource,
        'sqlite:///:memory:::t',
        connect_args={},
        dshape='var * {a: int32}',
    )
    assert s() is not s()


def test_nullable_foreign_key():
    """Test for issue #554"""
    engine = sa.create_engine('sqlite://')
    metadata = sa.MetaData(bind=engine)

    T1 = sa.Table(
        'NullableForeignKeyDemo',
        metadata,
        sa.Column('pkid', sa.Integer, primary_key=True),
        sa.Column('label_id', sa.Integer,
                  sa.ForeignKey("ForeignKeyLabels.pkid"), nullable=True),
    )

    T2 = sa.Table(
        'ForeignKeyLabels',
        metadata,
        sa.Column('pkid', sa.Integer, primary_key=True),
        sa.Column('label', sa.String),
    )

    metadata.create_all()

    x = np.arange(10)
    records1 = [
        {'pkid': idx, 'label_id': int(value)}
        for idx, value
        in enumerate(x[::-1])
    ]
    records1[-1]['label_id'] = None  # foreign-key is nullable!

    records2 = [
        {'pkid': int(pkid), 'label': chr(pkid + 65)}
        for pkid in x
    ]
    with engine.connect() as conn:
        conn.execute(T1.insert(), records1)
        conn.execute(T2.insert(), records2)

    ds = discover_sqlalchemy_selectable(T1)
    # The nullable key should be an Option instance
    assert isinstance(ds.measure['label_id'].key, Option)

    dtype = [('pkid', np.int32), ('label_id', object)]
    expected = np.rec.fromarrays([x, x[::-1]], dtype=dtype)
    expected = pd.DataFrame(expected)
    expected.iloc[-1, -1] = None

    actual = odo(T1, pd.DataFrame)
    assert actual.equals(expected)


def test_transaction():
    with tmpfile('.db') as fn:
        rsc = resource('sqlite:///%s::table' % fn, dshape='var * {a: int}')

    data = [(1,), (2,), (3,)]

    conn_1 = rsc.bind.connect()
    conn_2 = rsc.bind.connect()

    trans_1 = conn_1.begin()
    conn_2.begin()

    odo(data, rsc, bind=conn_1)

    # inside the transaction the write should be there
    assert odo(rsc, list, bind=conn_1) == data

    # outside of a transaction or in a different transaction the write is not
    # there
    assert odo(rsc, list) == odo(rsc, list, bind=conn_2) == []

    trans_1.commit()

    # now the data should appear outside the transaction
    assert odo(rsc, list) == odo(rsc, list, bind=conn_2) == data


def test_integer_detection():
    """Test for PR #596"""
    engine = sa.create_engine('sqlite://')
    metadata = sa.MetaData(bind=engine)

    T = sa.Table(
        'Demo', metadata,
        sa.Column('pkid', sa.Integer, primary_key=True),
        sa.Column(
            'value',
            sa.DECIMAL(precision=1, scale=0, asdecimal=False),
            nullable=True
        ),
    )
    metadata.create_all()

    values =  [1, 0, 1, 0, None, 1, 1, 1, 0, 1]
    pkids = range(len(values))
    dtype = [('pkid', np.int32), ('value', np.float64)]
    expected = np.array(list(zip(pkids, values)), dtype=dtype)
    expected = pd.DataFrame(expected)
    records = expected.to_dict(orient='records')
    with engine.connect() as conn:
        conn.execute(T.insert(), records)

    actual = odo(T, pd.DataFrame)
    assert actual.equals(expected)
