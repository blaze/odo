from __future__ import absolute_import, division, print_function

import re

from itertools import chain
from collections import Iterator
from datetime import datetime, date

import pandas as pd
import sqlalchemy as sa

import datashape
from datashape import DataShape, Record, Option, var, dshape
from datashape.predicates import isdimension, isrecord, isscalar
from datashape import discover
from datashape.dispatch import dispatch

from toolz import (partition_all, keyfilter, first, memoize, valfilter,
                   identity, concat, curry)
from toolz.curried import pluck, map

from ..utils import keywords, ignoring, iter_except
from ..convert import convert, ooc_types
from ..append import append
from ..resource import resource
from ..chunks import Chunks

base = (int, float, datetime, date, bool, str)


# http://docs.sqlalchemy.org/en/latest/core/types.html

types = {
    'int64': sa.types.BigInteger,
    'int32': sa.types.Integer,
    'int': sa.types.Integer,
    'int16': sa.types.SmallInteger,
    'float32': sa.types.Float(precision=24),  # sqlalchemy uses mantissa
    'float64': sa.types.Float(precision=53),  # for precision
    'float': sa.types.Float(precision=53),
    'real': sa.types.Float(precision=53),
    'string': sa.types.Text,
    'date': sa.types.Date,
    'time': sa.types.Time,
    'datetime': sa.types.DateTime,
    'bool': sa.types.Boolean,
    "timedelta[unit='D']": sa.types.Interval(second_precision=0,
                                             day_precision=9),
    "timedelta[unit='h']": sa.types.Interval(second_precision=0,
                                             day_precision=0),
    "timedelta[unit='m']": sa.types.Interval(second_precision=0,
                                             day_precision=0),
    "timedelta[unit='s']": sa.types.Interval(second_precision=0,
                                             day_precision=0),
    "timedelta[unit='ms']": sa.types.Interval(second_precision=3,
                                              day_precision=0),
    "timedelta[unit='us']": sa.types.Interval(second_precision=6,
                                              day_precision=0),
    "timedelta[unit='ns']": sa.types.Interval(second_precision=9,
                                              day_precision=0),
    # ??: sa.types.LargeBinary,
    # Decimal: sa.types.Numeric,
    # ??: sa.types.PickleType,
    # unicode: sa.types.Unicode,
    # unicode: sa.types.UnicodeText,
    # str: sa.types.Text,  # ??
}

revtypes = dict(map(reversed, types.items()))

revtypes.update({
    sa.types.VARCHAR: 'string',
    sa.types.String: 'string',
    sa.types.Unicode: 'string',
    sa.types.DATETIME: 'datetime',
    sa.types.TIMESTAMP: 'datetime',
    sa.types.FLOAT: 'float64',
    sa.types.DATE: 'date',
    sa.types.BIGINT: 'int64',
    sa.types.INTEGER: 'int',
    sa.types.NUMERIC: 'float64',  # TODO: extend datashape to decimal
    sa.types.BIGINT: 'int64',
    sa.types.NullType: 'string',
    sa.types.Float: 'float64',
})

# interval types are special cased in discover_typeengine so remove them from
# revtypes
revtypes = valfilter(lambda x: not isinstance(x, sa.types.Interval), revtypes)


units_of_power = {
    0: 's',
    3: 'ms',
    6: 'us',
    9: 'ns'
}

# these aren't loaded by sqlalchemy by default
sa.dialects.registry.load('oracle')
sa.dialects.registry.load('postgresql')


def batch(sel, chunksize=10000):
    """Execute `sel`, streaming row at a time and fetching from the database in
    batches of size `chunksize`.

    Parameters
    ----------
    sel : sa.sql.Selectable
        Selectable to execute
    chunksize : int, optional, default 10000
        Number of rows to fetch from the database
    """
    def rowterator(sel, chunksize=chunksize):
        with sel.bind.connect() as conn:
            result = conn.execute(sel)
            yield result.keys()

            for rows in iter_except(curry(result.fetchmany, size=chunksize),
                                    sa.exc.ResourceClosedError):
                if rows:
                    yield rows
                else:
                    return
    terator = rowterator(sel)
    return next(terator), concat(terator)


@discover.register(sa.dialects.postgresql.base.INTERVAL)
def discover_postgresql_interval(t):
    return discover(sa.types.Interval(day_precision=0,
                                      second_precision=t.precision))


@discover.register(sa.dialects.oracle.base.INTERVAL)
def discover_oracle_interval(t):
    return discover(t.adapt(sa.types.Interval))


@discover.register(sa.sql.type_api.TypeEngine)
def discover_typeengine(typ):
    if isinstance(typ, sa.types.Interval):
        if typ.second_precision is None and typ.day_precision is None:
            return datashape.TimeDelta(unit='us')
        elif typ.second_precision == 0 and typ.day_precision == 0:
            return datashape.TimeDelta(unit='s')

        if typ.second_precision in units_of_power and not typ.day_precision:
            units = units_of_power[typ.second_precision]
        elif typ.day_precision > 0:
            units = 'D'
        else:
            raise ValueError('Cannot infer INTERVAL type with parameters'
                             'second_precision=%d, day_precision=%d' %
                             (typ.second_precision, typ.day_precision))
        return datashape.TimeDelta(unit=units)
    if typ in revtypes:
        return dshape(revtypes[typ])[0]
    if type(typ) in revtypes:
        return dshape(revtypes[type(typ)])[0]
    else:
        for k, v in revtypes.items():
            if isinstance(k, type) and (isinstance(typ, k) or
                                        hasattr(typ, 'impl') and
                                        isinstance(typ.impl, k)):
                return v
            if k == typ:
                return v
    raise NotImplementedError("No SQL-datashape match for type %s" % typ)


@discover.register(sa.Column)
def discover_sqlalchemy_column(col):
    optionify = Option if col.nullable else identity
    return Record([[col.name, optionify(discover(col.type))]])


@discover.register(sa.sql.FromClause)
def discover_sqlalchemy_selectable(t):
    records = list(sum([discover(c).parameters[0] for c in t.columns], ()))
    return var * Record(records)


@memoize
def metadata_of_engine(engine):
    return sa.MetaData(engine)


def create_engine(uri, *args, **kwargs):
    if ':memory:' in uri:
        return sa.create_engine(uri, *args, **kwargs)
    else:
        return memoized_create_engine(uri, *args, **kwargs)


memoized_create_engine = memoize(sa.create_engine)


@dispatch(sa.engine.base.Engine, str)
def discover(engine, tablename):
    metadata = metadata_of_engine(engine)
    if tablename not in metadata.tables:
        try:
            metadata.reflect(engine,
                             views=metadata.bind.dialect.supports_views)
        except NotImplementedError:
            metadata.reflect(engine)
    table = metadata.tables[tablename]
    return discover(table)


@dispatch(sa.engine.base.Engine)
def discover(engine):
    metadata = metadata_of_engine(engine)
    return discover(metadata)


@dispatch(sa.MetaData)
def discover(metadata):
    try:
        metadata.reflect(views=metadata.bind.dialect.supports_views)
    except NotImplementedError:
        metadata.reflect()
    pairs = []
    for name, table in sorted(metadata.tables.items(), key=first):
        try:
            pairs.append([name, discover(table)])
        except sa.exc.CompileError as e:
            print("Can not discover type of table %s.\n" % name +
                  "SQLAlchemy provided this error message:\n\t%s" % e.message +
                  "\nSkipping.")
        except NotImplementedError as e:
            print("Blaze does not understand a SQLAlchemy type.\n"
                  "Blaze provided the following error:\n\t%s" % "\n\t".join(e.args) +
                  "\nSkipping.")
    return DataShape(Record(pairs))


@discover.register(sa.engine.RowProxy)
def discover_row_proxy(rp):
    return Record(list(zip(rp.keys(), map(discover, rp.values()))))


def dshape_to_table(name, ds, metadata=None):
    """
    Create a SQLAlchemy table from a datashape and a name

    >>> dshape_to_table('bank', '{name: string, amount: int}') # doctest: +NORMALIZE_WHITESPACE
    Table('bank', MetaData(bind=None),
          Column('name', Text(), table=<bank>, nullable=False),
          Column('amount', Integer(), table=<bank>, nullable=False),
          schema=None)
    """

    if isinstance(ds, str):
        ds = dshape(ds)
    metadata = metadata or sa.MetaData()
    cols = dshape_to_alchemy(ds)
    return sa.Table(name, metadata, *cols)


@dispatch(object, str)
def create_from_datashape(o, ds, **kwargs):
    return create_from_datashape(o, dshape(ds), **kwargs)


@dispatch(sa.engine.base.Engine, DataShape)
def create_from_datashape(engine, ds, **kwargs):
    assert isrecord(ds)
    metadata = metadata_of_engine(engine)
    for name, sub_ds in ds[0].dict.items():
        t = dshape_to_table(name, sub_ds, metadata=metadata)
        t.create()
    return engine


def dshape_to_alchemy(dshape):
    """

    >>> dshape_to_alchemy('int')
    <class 'sqlalchemy.sql.sqltypes.Integer'>

    >>> dshape_to_alchemy('string')
    <class 'sqlalchemy.sql.sqltypes.Text'>

    >>> dshape_to_alchemy('{name: string, amount: int}')
    [Column('name', Text(), table=None, nullable=False), Column('amount', Integer(), table=None, nullable=False)]

    >>> dshape_to_alchemy('{name: ?string, amount: ?int}')
    [Column('name', Text(), table=None), Column('amount', Integer(), table=None)]
    """
    if isinstance(dshape, str):
        dshape = datashape.dshape(dshape)
    if isinstance(dshape, Option):
        return dshape_to_alchemy(dshape.ty)
    if str(dshape) in types:
        return types[str(dshape)]
    if isinstance(dshape, datashape.Record):
        return [sa.Column(name,
                          dshape_to_alchemy(typ),
                          nullable=isinstance(typ[0], Option))
                for name, typ in dshape.parameters[0]]
    if isinstance(dshape, datashape.DataShape):
        if isdimension(dshape[0]):
            return dshape_to_alchemy(dshape[1])
        else:
            return dshape_to_alchemy(dshape[0])
    if isinstance(dshape, datashape.String):
        if dshape[0].fixlen is None:
            return sa.types.Text
        if 'U' in dshape.encoding:
            return sa.types.Unicode(length=dshape[0].fixlen)
        if 'A' in dshape.encoding:
            return sa.types.String(length=dshape[0].fixlen)
    if isinstance(dshape, datashape.DateTime):
        if dshape.tz:
            return sa.types.DateTime(timezone=True)
        else:
            return sa.types.DateTime(timezone=False)
    raise NotImplementedError("No SQLAlchemy dtype match for datashape: %s"
                              % dshape)


@convert.register(Iterator, sa.Table, cost=300.0)
def sql_to_iterator(t, **kwargs):
    _, rows = batch(sa.select([t]))
    return map(tuple, rows)


@convert.register(Iterator, sa.sql.Select, cost=300.0)
def select_to_iterator(sel, dshape=None, **kwargs):
    func = pluck(0) if dshape and isscalar(dshape.measure) else map(tuple)
    _, rows = batch(sel)
    return func(rows)


@convert.register(base, sa.sql.Select, cost=300.0)
def select_to_base(sel, dshape=None, **kwargs):
    assert not dshape or isscalar(dshape), \
        'dshape should be None or scalar, got %s' % dshape
    with sel.bind.connect() as conn:
        return conn.execute(sel).scalar()


@append.register(sa.Table, Iterator)
def append_iterator_to_table(t, rows, dshape=None, **kwargs):
    assert not isinstance(t, type)
    rows = iter(rows)

    # We see if the sequence is of tuples or dicts
    # If tuples then we coerce them to dicts
    try:
        row = next(rows)
    except StopIteration:
        return
    rows = chain([row], rows)
    if isinstance(row, (tuple, list)):
        if dshape and isinstance(dshape.measure, datashape.Record):
            names = dshape.measure.names
            if set(names) != set(discover(t).measure.names):
                raise ValueError("Column names of incoming data don't match "
                                 "column names of existing SQL table\n"
                                 "Names in SQL table: %s\n"
                                 "Names from incoming data: %s\n" %
                                 (discover(t).measure.names, names))
        else:
            names = discover(t).measure.names
        rows = (dict(zip(names, row)) for row in rows)

    engine = t.bind
    with engine.connect() as conn:
        for chunk in partition_all(1000, rows):  # TODO: 1000 is hardcoded
            conn.execute(t.insert(), chunk)

    return t


@append.register(sa.Table, Chunks)
def append_anything_to_sql_Table(t, c, **kwargs):
    for item in c:
        append(t, item, **kwargs)


@append.register(sa.Table, object)
def append_anything_to_sql_Table(t, o, **kwargs):
    return append(t, convert(Iterator, o, **kwargs), **kwargs)


@append.register(sa.Table, sa.Table)
def append_table_to_sql_Table(t, o, **kwargs):
    # This condition is an ugly kludge and should be removed once
    # https://github.com/dropbox/PyHive/issues/15 is resolved
    if t.bind.name == o.bind.name == 'hive':
        with t.bind.connect() as conn:
            conn.execute('INSERT INTO TABLE %s SELECT * FROM %s' %
                         (t.name, o.name))
        return t

    s = sa.select([o])
    return append(t, s, **kwargs)


@append.register(sa.Table, sa.sql.Select)
def append_select_statement_to_sql_Table(t, o, **kwargs):
    if not o.bind == t.bind:
        return append(t, convert(Iterator, o, **kwargs), **kwargs)

    assert o.bind.has_table(t.name), 'tables must come from the same database'

    query = t.insert().from_select(o.columns.keys(), o)

    with o.bind.connect() as conn:
        conn.execute(query)
    return t


@resource.register('(.*sql.*|oracle|redshift)(\+\w+)?://.+')
def resource_sql(uri, *args, **kwargs):
    kwargs2 = keyfilter(keywords(sa.create_engine).__contains__, kwargs)
    engine = create_engine(uri, **kwargs2)
    ds = kwargs.get('dshape')

    # we were also given a table name
    if args and isinstance(args[0], str):
        table_name, args = args[0], args[1:]
        metadata = metadata_of_engine(engine)
        with ignoring(sa.exc.NoSuchTableError):
            return sa.Table(table_name, metadata, autoload=True,
                            autoload_with=engine)
        if ds:
            t = dshape_to_table(table_name, ds, metadata)
            t.create()
            return t
        else:
            raise ValueError("Table does not exist and no dshape provided")

    # We were not given a table name
    if ds:
        create_from_datashape(engine, ds)
    return engine


@resource.register('impala://.+')
def resource_impala(uri, *args, **kwargs):
    try:
        import impala.sqlalchemy
    except ImportError:
        raise ImportError("Please install or update `impyla` library")
    return resource_sql(uri, *args, **kwargs)


@resource.register('monetdb://.+')
def resource_monet(uri, *args, **kwargs):
    try:
        import monetdb
    except ImportError:
        raise ImportError("Please install the `sqlalchemy_monetdb` library")
    return resource_sql(uri, *args, **kwargs)


@resource.register('hive://.+')
def resource_hive(uri, *args, **kwargs):
    try:
        import pyhive
    except ImportError:
        raise ImportError("Please install the `PyHive` library.")

    pattern = 'hive://((?P<user>[a-zA-Z_]\w*)@)?(?P<host>[\w.]+)(:(?P<port>\d*))?(/(?P<database>\w*))?'
    d = re.search(pattern, uri.split('::')[0]).groupdict()

    defaults = {'port': '10000',
                'user': 'hdfs',
                'database': 'default'}
    for k, v in d.items():
        if not v:
            d[k] = defaults[k]

    if d['user']:
        d['user'] += '@'

    uri2 = 'hive://%(user)s%(host)s:%(port)s/%(database)s' % d
    if '::' in uri:
        uri2 += '::' + uri.split('::')[1]

    return resource_sql(uri2, *args, **kwargs)


ooc_types.add(sa.Table)


@dispatch(sa.Table)
def drop(table):
    table.drop(table.bind, checkfirst=True)


@convert.register(pd.DataFrame, (sa.sql.Select, sa.sql.Selectable), cost=200.0)
def select_or_selectable_to_frame(el, **kwargs):
    columns, rows = batch(el)
    row = next(rows, None)
    if row is None:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(list(chain([tuple(row)], map(tuple, rows))),
                        columns=columns)
