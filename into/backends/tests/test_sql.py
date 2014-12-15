from __future__ import absolute_import, division, print_function

import sqlalchemy as sa
from datashape import discover, dshape
import datashape
from into.backends.sql import (dshape_to_table, create_from_datashape,
        dshape_to_alchemy)
from into.utils import tmpfile
from into import convert, append, create, resource, discover


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


def test_discovery():
    assert discover(sa.String()) == datashape.string
    metadata = sa.MetaData()
    s = sa.Table('accounts', metadata,
                 sa.Column('name', sa.String),
                 sa.Column('amount', sa.Integer),
                 sa.Column('timestamp', sa.DateTime, primary_key=True))

    assert discover(s) == \
            dshape('var * {name: ?string, amount: ?int32, timestamp: datetime}')

def test_discovery_numeric_column():
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


def single_table_engine():
    engine = sa.create_engine('sqlite:///:memory:')
    metadata = sa.MetaData(engine)
    t = sa.Table('accounts', metadata,
                 sa.Column('name', sa.String),
                 sa.Column('amount', sa.Integer))
    t.create()
    return engine, t


def test_discovery_engine():
    engine, t = single_table_engine()

    assert discover(engine, 'accounts') == discover(t)

    assert str(discover(engine)) == str(discover({'accounts': t}))


def test_discovery_metadata():
    engine, t = single_table_engine()
    metadata = t.metadata
    assert str(discover(metadata)) == str(discover({'accounts': t}))


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


    t2 = dshape_to_table('points2', '{x: int, y: int}', metadata=metadata)
    t2.create()
    data2 = [{'x': 1, 'y': 1}, {'x': 2, 'y': 4}, {'x': 3, 'y': 9}]
    append(t2, data2)

    assert convert(list, t2) == data
