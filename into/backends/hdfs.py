from __future__ import absolute_import, division, print_function

from toolz import memoize, merge
from functools import wraps

from .csv import CSV
import datashape
import sqlalchemy as sa
from datashape import discover
from datashape import coretypes as ct
from pywebhdfs.webhdfs import PyWebHdfsClient
from collections import namedtuple
from contextlib import contextmanager
from .sql import metadata_of_engine, sa
from ..utils import tmpfile, sample
from ..append import append
from ..resource import resource
from ..directory import _Directory, Directory


class _HDFS(object):
    """ Parent class for data on Hadoop File System

    Examples
    --------

    >>> from into import HDFS, CSV
    >>> HDFS(CSV)('/path/to/file.csv')
    """
    def __init__(self, *args, **kwargs):
        hdfs = kwargs.pop('hdfs', None)
        host = kwargs.pop('host', None)
        port = kwargs.pop('port', '14000')
        user = kwargs.pop('user', 'hdfs')

        if not hdfs and (host and port and user):
            hdfs = PyWebHdfsClient(host=host, port=port, user_name=user)

        if hdfs is None:
            raise ValueError("No HDFS credentials found.\n"
                    "Either supply a PyWebHdfsClient instance or keywords\n"
                    "   host=, port=, user=")
        self.hdfs = hdfs

        self.subtype.__init__(self, *args, **kwargs)


@memoize
def HDFS(cls):
    return type('HDFS(%s)' % cls.__name__, (_HDFS, cls), {'subtype':  cls})


@sample.register(HDFS(CSV))
@contextmanager
def sample_hdfs_csv(data, length=10000):
    sample = data.hdfs.read_file(data.path.lstrip('/'), length=length)
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write(sample)

        yield fn


@discover.register(HDFS(CSV))
def discover_hdfs_csv(data, length=10000, **kwargs):
    with sample(data, length=length) as fn:
        result = discover(CSV(fn, encoding=data.encoding,
                                  dialect=data.dialect,
                                  has_header=data.has_header))
    return result


@sample.register(HDFS(Directory(CSV)))
@contextmanager
def sample_hdfs_directory_csv(data, **kwargs):
    files = data.hdfs.list_dir(data.path.lstrip('/'))
    one_file = data.path + '/' + files['FileStatuses']['FileStatus'][0]['pathSuffix']
    csv = HDFS(CSV)(one_file, hdfs=data.hdfs)
    with sample(csv, **kwargs) as fn:
        yield fn


@discover.register(HDFS(Directory(CSV)))
def discover_hdfs_directory(data, length=10000, **kwargs):
    with sample(data, length=length) as fn:
        o = data.container(fn, **data.kwargs)
        result = discover(o)
    return result

"""
Hive Tables
===========

Hive acts a bit differently from other databases that we interact with through
SQL.  As a result we need to special case a lot of code.

Foremost, a database is just a directory on HDFS holding something like a CSV
file (or Avro, Parquet, etc..)  As a result when we construct a Table we
actually have to know a lot about our CSV files (e.g. delimiter.)

This breaks the current into model a bit because we usually create things with
`resource` and then `append` on to them.  Here we need to know both at the
same time.  Enter a convenient hack, a token for a proxy table
"""

TableProxy = namedtuple('TableProxy', 'engine,name')

"""
resource('hive://...::tablename') now gives us one of these.  The
subsequent call to `append` does the actual creation.

We're looking for better solutions.  For the moment, this works.
"""

@resource.register('hive://.+::.+', priority=16)
def resource_hive_table(uri, **kwargs):
    uri, table = uri.split('::')
    engine = resource(uri)
    metadata = metadata_of_engine(engine)
    if table in metadata.tables:
        return metadata.tables[table]
    metadata.reflect(engine, views=False)
    if table in metadata.tables:
        return metadata.tables[table]
    return TableProxy(engine, table)


hive_types = {
        ct.int8: 'TINYINT',
        ct.int16: 'SMALLINT',
        ct.int32: 'INT',
        ct.int64: 'BIGINT',
        ct.float32: 'FLOAT',
        ct.float64: 'DOUBLE',
        ct.date_: 'DATE',
        ct.datetime_: 'TIMESTAMP',
        ct.string: 'STRING',
        ct.bool_: 'BOOLEAN'}


def dshape_to_hive(ds):
    """ Convert datashape measure to Hive dtype string

    >>> dshape_to_hive('int16')
    'SMALLINT'
    >>> dshape_to_hive('?int32')  # Ignore option types
    'INT'
    >>> dshape_to_hive('string[256]')
    'VARCHAR(256)'
    """
    if isinstance(ds, (str, unicode)):
        ds = datashape.dshape(ds)
    if isinstance(ds, ct.DataShape):
        ds = ds.measure
    if isinstance(ds, ct.Option):
        ds = ds.ty
    if isinstance(ds, ct.String):
        if ds.fixlen:
            return 'VARCHAR(%d)' % ds.fixlen
        else:
            return 'STRING'
    if ds in hive_types:
        return hive_types[ds]
    raise NotImplementedError("No Hive dtype known for %s" % ds)



"""
Load Data in HDFS into Hive
===========================

We push types like HDFS(CSV) and HDFS(Directory(CSV)) into Hive tables.  This
requires that we bring a bit of the CSV file locally, inspect it (sniff for csv
dialect), generate the appropriate CREATE EXTERNAL TABLE command, and then
execute.
"""

import csv
sniffer = csv.Sniffer()

def create_hive_from_hdfs_directory_of_csvs(tbl, data, dshape=None, **kwargs):
    """
    Generate string to create Hive table

    Most of the logic is here.  Also easy to test.
    """
    path = data.path
    tblname = tbl.name

    dialect = merge(data.kwargs, kwargs)

    # Get sample text
    with sample(data, length=30000) as fn:
        text = open(fn).read()

    # Handle header, dialect
    if 'has_header' not in dialect:
        dialect['has_header'] = sniffer.has_header(text)

    d = sniffer.sniff(text)
    delimiter = dialect.get('delimiter', d.delimiter)
    escapechar = dialect.get('escapechar', d.escapechar)
    lineterminator = dialect.get('lineterminator', d.lineterminator)
    quotechar = dialect.get('quotechar', d.quotechar)

    dbname = str(tbl.engine.url).split('/')[-1]
    if dbname:
        dbname = dbname + '.'

    # Column names and types from datashape
    ds = dshape or discover(data)
    assert isinstance(ds.measure, ct.Record)
    columns = [(name, dshape_to_hive(typ))
            for name, typ in zip(ds.measure.names, ds.measure.types)]
    column_text = ',\n'.join('%30s  %s' % col for col in columns)[12:]

    statement = """
        CREATE EXTERNAL TABLE {dbname}{tblname} (
            {column_text}
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '{delimiter}'
        STORED AS TEXTFILE
        LOCATION '{path}'
        """

    if dialect.get('has_header'):
        statement = statement + """
        TBLPROPERTIES ("skip.header.line.count"="1")"""

    return statement.format(**locals())


@append.register(TableProxy, HDFS(Directory(CSV)))
def create_new_hive_table_from_csv(tbl, data, **kwargs):
    """
    Create new Hive table from directory of CSV files on HDFS

    Actually executes command.
    """
    if tbl.engine.dialect.name == 'hive':
        statement = create_hive_from_hdfs_directory_of_csvs(tbl, data, **kwargs)
    else:
        raise NotImplementedError("Don't know how to migrate directory of csvs"
                " on HDFS to database of dialect %s" % tbl.engine.dialect.name)

    with tbl.engine.connect() as conn:
        conn.execute(statement)

    metadata = metadata_of_engine(tbl.engine)
    tbl2 = sa.Table(tbl.name, metadata, autoload=True,
            autoload_with=tbl.engine)
    return tbl2


"""
Load Remote data into Hive
==========================

We push types like SSH(CSV) and SSH(Directory(CSV)) into Hive tables.

Often our data is on the cluster but not on HDFS.  There are slightly different
CREATE TABLE and LOAD commands in this case.

Note that there is a *lot* of code duplication to what we just saw above.  I'm
not sure yet how best to refactor this.  There are a lot of annoying bits to
work around that have so far pushed me to write ugly code.
"""
from .ssh import SSH

def create_hive_from_remote_csv_file(tbl, data, dshape=None, **kwargs):
    path = data.path
    tblname = tbl.name

    dialect = merge(dialect_of(data), kwargs)

    # Get sample text
    with sample(data) as fn:
        text = open(fn).read()

    # Handle header, dialect
    if 'has_header' not in dialect:
        dialect['has_header'] = sniffer.has_header(text)

    d = sniffer.sniff(text)
    delimiter = dialect.get('delimiter', d.delimiter)
    escapechar = dialect.get('escapechar', d.escapechar)
    lineterminator = dialect.get('lineterminator', d.lineterminator)
    quotechar = dialect.get('quotechar', d.quotechar)

    dbname = str(tbl.engine.url).split('/')[-1]
    if dbname:
        dbname = dbname + '.'

    # Column names and types from datashape
    ds = dshape or discover(data)
    assert isinstance(ds.measure, ct.Record)
    columns = [(name, dshape_to_hive(typ))
            for name, typ in zip(ds.measure.names, ds.measure.types)]
    column_text = ',\n'.join('%30s  %s' % col for col in columns)[12:]

    statement = """
        CREATE TABLE {dbname}{tblname} (
            {column_text}
            )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '{delimiter}'
        STORED AS TEXTFILE
        """

    if dialect.get('has_header'):
        statement = statement + """
        TBLPROPERTIES ("skip.header.line.count"="1")"""

    return statement.format(**locals())


@append.register(TableProxy, (SSH(CSV), SSH(Directory(CSV))))
def create_and_append_remote_csv_to_table(tbl, data, **kwargs):
    if tbl.engine.dialect.name == 'hive':
        statement = create_hive_from_remote_csv_file(tbl, data, **kwargs)
    else:
        raise NotImplementedError("Don't know how to migrate csvs on remote "
                "disk to database of dialect %s" % tbl.engine.dialect.name)

    with tbl.engine.connect() as conn:
        conn.execute(statement)

    metadata = metadata_of_engine(tbl.engine)
    tbl2 = sa.Table(tbl.name, metadata, autoload=True,
            autoload_with=tbl.engine)
    return append(tbl2, data, **kwargs)


@append.register(sa.Table, (SSH(CSV), SSH(Directory(CSV))))
def append_remote_csv_to_table(tbl, csv, **kwargs):
    if tbl.bind.dialect.name == 'hive':
        statement = ('LOAD DATA LOCAL INPATH "%(path)s" INTO TABLE %(tablename)s' %
                {'path': csv.path, 'tablename': tbl.name})
    else:
        raise NotImplementedError("Don't know how to migrate csvs on remote "
                "disk to database of dialect %s" % tbl.engine.dialect.name)
    with tbl.bind.connect() as conn:
        conn.execute(statement)
    return tbl


def dialect_of(data):
    if isinstance(data, CSV):
        return data.dialect
    if isinstance(data, _Directory):
        return data.kwargs
    raise NotImplementedError()
