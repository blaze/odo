from __future__ import absolute_import, division, print_function

from toolz import memoize, merge
from functools import wraps

from .csv import CSV
import datashape
from datashape import discover
from datashape import coretypes as ct
from pywebhdfs.webhdfs import PyWebHdfsClient
from collections import namedtuple
from contextlib import contextmanager
from .sql import metadata_of_engine, sa
from ..utils import tmpfile
from ..append import append
from ..resource import resource
from ..directory import _Directory, Directory

from multipledispatch import Dispatcher
sample = Dispatcher('sample')

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


hive_types = {
        ct.int8: 'TINYINT',
        ct.int16: 'SMALLINT',
        ct.int32: 'INT',
        ct.int64: 'SMALLINT',
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
    >>> dshape_to_hive('?int16')  # Ignore option types
    'SMALLINT'
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


import csv
sniffer = csv.Sniffer()


from ..regex import RegexDispatcher
create_command = RegexDispatcher('create_command')

@create_command.register('hive')
def create_hive(dialect, tbl, data, dshape=None, **kwargs):
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


@resource.register('hive://.+::.+', priority=16)
def resource_hive_table(uri, **kwargs):
    uri, table = uri.split('::')
    engine = resource(uri, **kwargs)
    metadata = metadata_of_engine(engine)
    if table in metadata.tables:
        return metadata.tables[table]
    metadata.reflect(engine, views=False)
    if table in metadata.tables:
        return metadata.tables[table]
    return TableProxy(engine, table)


TableProxy = namedtuple('TableProxy', 'engine,name')


@append.register(TableProxy, HDFS(Directory(CSV)))
def create_new_hive_table_from_csv(tbl, data, **kwargs):
    statement = create_command(tbl.engine.dialect.name, tbl, data, **kwargs)
    with tbl.engine.connect() as conn:
        conn.execute(statement)

    metadata = metadata_of_engine(tbl.engine)
    tbl2 = sa.Table(tbl.name, metadata, autoload=True,
            autoload_with=tbl.engine)
    return tbl2
