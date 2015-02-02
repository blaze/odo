from __future__ import absolute_import, division, print_function

from toolz import memoize, merge
from functools import wraps
import re

from .csv import CSV
from .json import JSON, JSONLines
import datashape
import sqlalchemy as sa
from datashape import discover, dshape
from datashape import coretypes as ct
from collections import namedtuple
from contextlib import contextmanager
from .ssh import SSH, _SSH
from .sql import metadata_of_engine, sa
from ..utils import tmpfile, sample, ignoring
from ..append import append
from ..resource import resource
from ..directory import _Directory, Directory
from ..compatibility import unicode


with ignoring(ImportError):
    from pywebhdfs.webhdfs import PyWebHdfsClient


class _HDFS(object):
    """ Parent class for data on Hadoop File System

    Examples
    --------

    >>> HDFS(CSV)('/path/to/file.csv', host='54.91.255.255',
    ...           port=14000, user='hdfs')  # doctest: +SKIP

    Alternatively use resource strings

    >>> resource('hdfs://hdfs@54.91.255.255:/path/to/file.csv')  # doctest: +SKIP
    """
    def __init__(self, *args, **kwargs):
        hdfs = kwargs.pop('hdfs', None)
        host = kwargs.pop('host', None)
        port = str(kwargs.pop('port', '14000'))
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


def create_hive_statement(tbl_name, dshape, path=None, table_type='',
        db_name='default', **dialect):
    """ Generic CREATE TABLE statement for hive

    Parameters
    ----------

    tbl_name : string
        Specifies table name "mytable"
    dshape : DataShape
        Datashape of the desired table
    path : string (optional)
        Location of data
    table_type : string (optional)
        Table Modifier like EXTERNAL or LOCAL
    db_name : string
        Specifies database name.  Defaults to "default"

    **dialect : keyword arguments dict
        CSV dialect with keys delimiter, has_header, etc.

    Example
    -------

    >>> ds = dshape('var * {name: string, balance: int64, when: datetime}')
    >>> print(create_hive_statement('accounts', ds, delimiter=','))  # doctest: +NORMALIZE_WHITESPACE
    CREATE  TABLE default.accounts (
            name  STRING,
         balance  BIGINT,
            when  TIMESTAMP
    )
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE

    >>> print(create_hive_statement('accounts', ds, delimiter=',',
    ...         has_header=True, path='/data/accounts/', table_type='EXTERNAL'))  # doctest: +NORMALIZE_WHITESPACE
    CREATE EXTERNAL TABLE default.accounts (
            name  STRING,
         balance  BIGINT,
            when  TIMESTAMP
    )
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION '/data/accounts/'
    TBLPROPERTIES ("skip.header.line.count"="1")
    """
    if db_name:
        db_name = db_name + '.'

    if not table_type:
        table_type = ''

    # Column names and types from datashape
    ds = dshape or discover(data)
    assert isinstance(ds.measure, ct.Record)
    columns = [(name, dshape_to_hive(typ))
            for name, typ in zip(ds.measure.names, ds.measure.types)]
    column_text = ',\n    '.join('%20s  %s' % col for col in columns).lstrip()

    statement = """
        CREATE {table_type} TABLE {db_name}{tbl_name} (
            {column_text}
        )
        ROW FORMAT DELIMITED
            FIELDS TERMINATED BY '{delimiter}'
        STORED AS TEXTFILE
        """

    if path:
        statement = statement +"""
        LOCATION '{path}'
        """

    if dialect.get('has_header'):
        statement = statement + """
        TBLPROPERTIES ("skip.header.line.count"="1")"""

    return statement.format(**merge(dialect, locals())).strip('\n')


"""
Load Data from HDFS or SSH into Hive
====================================

We push types like HDFS(CSV) and HDFS(Directory(CSV)) into Hive tables.  This
requires that we bring a bit of the CSV file locally, inspect it (sniff for csv
dialect), generate the appropriate CREATE EXTERNAL TABLE command, and then
execute.
"""


@append.register(TableProxy, HDFS(Directory(CSV)))
def create_new_hive_table_from_csv(tbl, data, dshape=None, path=None, **kwargs):
    """
    Create new Hive table from directory of CSV files on HDFS

    Actually executes command.
    """
    if isinstance(data, _SSH):
        table_type = None
    elif isinstance(data, _HDFS):
        table_type = 'EXTERNAL'
    else:
        table_type = None

    if not dshape:
        dshape = discover(data)

    if tbl.engine.dialect.name == 'hive':
        statement = create_hive_statement(tbl.name, dshape,
                                path=data.path,
                                db_name = str(tbl.engine.url).split('/')[-1],
                                table_type=table_type,
                                **dialect_of(data))
    else:
        raise NotImplementedError("Don't know how to migrate directory of csvs"
                " on HDFS to database of dialect %s" % tbl.engine.dialect.name)

    with tbl.engine.connect() as conn:
        conn.execute(statement)

    metadata = metadata_of_engine(tbl.engine)
    tbl2 = sa.Table(tbl.name, metadata, autoload=True,
            autoload_with=tbl.engine)
    return tbl2


@append.register(TableProxy, (SSH(CSV), SSH(Directory(CSV))))
def append_remote_csv_to_new_table(tbl, data, dshape=None, **kwargs):
    if not dshape:
        dshape = discover(data)

    if tbl.engine.dialect.name == 'hive':
        statement = create_hive_statement(tbl.name, dshape,
                                db_name = str(tbl.engine.url).split('/')[-1],
                                **dialect_of(data))
    else:
        raise NotImplementedError("Don't know how to migrate directory of csvs"
                " on Local disk to database of dialect %s" % tbl.engine.dialect.name)

    with tbl.engine.connect() as conn:
        conn.execute(statement)

    metadata = metadata_of_engine(tbl.engine)
    tbl2 = sa.Table(tbl.name, metadata, autoload=True,
            autoload_with=tbl.engine)

    return append(tbl2, data, **kwargs)


@append.register(sa.Table, (SSH(CSV), SSH(Directory(CSV))))
def append_remote_csv_to_table(tbl, csv, **kwargs):
    """
    Load Remote data into existing Hive table
    """
    path = csv.path
    if path[0] != '/':
        path = '/home/%s/%s' % (csv.auth['username'], csv.path)

    if tbl.bind.dialect.name == 'hive':
        statement = ('LOAD DATA LOCAL INPATH "%(path)s" INTO TABLE %(tablename)s' %
                {'path': path, 'tablename': tbl.name})
    else:
        raise NotImplementedError("Don't know how to migrate csvs on remote "
                "disk to database of dialect %s" % tbl.engine.dialect.name)
    with tbl.bind.connect() as conn:
        conn.execute(statement)
    return tbl


import csv
sniffer = csv.Sniffer()


def dialect_of(data, **kwargs):
    """ CSV dialect of a CSV file stored in SSH, HDFS, or a Directory. """
    keys = set(['delimiter', 'doublequote', 'escapechar', 'lineterminator',
                'quotechar', 'quoting', 'skipinitialspace', 'strict', 'has_header'])
    if isinstance(data, (HDFS(CSV), SSH(CSV))):
        with sample(data) as fn:
            d = dialect_of(CSV(fn, **data.dialect))
    elif isinstance(data, (HDFS(Directory(CSV)), SSH(Directory(CSV)))):
        with sample(data) as fn:
            d = dialect_of(CSV(fn, **data.kwargs))
    elif isinstance(data, Directory(CSV)):
        d = dialect_of(next(data))
    else:
        assert isinstance(data, CSV)

        # Get sample text
        with open(data.path, 'r') as f:
            text = f.read(1000)

        result = dict()

        d = sniffer.sniff(text)
        d = dict((k, getattr(d, k)) for k in keys if hasattr(d, k))

        if data.has_header is None:
            d['has_header'] = sniffer.has_header(text)

        d.update(data.dialect)

    d.update(kwargs)
    d = dict((k, v) for k, v in d.items() if k in keys)

    return d



types_by_extension = {'csv': CSV, 'json': JSONLines}

hdfs_pattern = '(((?P<user>[a-zA-Z]\w*)@)?(?P<host>[\w.-]*)?(:(?P<port>\d+))?:)?(?P<path>[/\w.*-]+)'

@resource.register('hdfs://.*', priority=16)
def resource_hdfs(uri, **kwargs):
    if 'hdfs://' in uri:
        uri = uri[len('hdfs://'):]

    d = re.match(hdfs_pattern, uri).groupdict()
    d = dict((k, v) for k, v in d.items() if v is not None)
    path = d.pop('path')

    kwargs.update(d)

    try:
        subtype = types_by_extension[path.split('.')[-1]]
        if '*' in path:
            subtype = Directory(subtype)
            path = path.rsplit('/', 1)[0] + '/'
    except KeyError:
        subtype = type(resource(path))

    return HDFS(subtype)(path, **kwargs)
