from toolz import memoize
from functools import wraps

from .csv import CSV
import datashape
from datashape import discover
from datashape import coretypes as ct
from pywebhdfs.webhdfs import PyWebHdfsClient
from ..utils import tmpfile

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


@discover.register(HDFS(CSV))
def discover_hdfs_csv(data, length=10000, **kwargs):
    sample = data.hdfs.read_file(data.path.lstrip('/'), length=length)
    with tmpfile('.csv') as fn:
        with open(fn, 'w') as f:
            f.write(sample)
        result = discover(CSV(fn, encoding=data.encoding,
                                  dialect=data.dialect,
                                  has_header=data.has_header))
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


from ..regex import RegexDispatcher
create_command = RegexDispatcher('create_command')

@create_command.register('hive')
def copy_hive(dialect, tbl, csv, dshape=None):
    path = csv.path
    tblname = tbl.table_name
    dbname = tbl.db_name
    if dbname:
        dbname = dbname + '.'
    delimiter = csv.dialect.get('delimiter', ',')
    quotechar = csv.dialect.get('quotechar', '"')
    escapechar = csv.dialect.get('escapechar', '\\')
    encoding = csv.encoding or 'utf-8'

    ds = dshape or discover(csv)
    assert isinstance(ds.measure, ct.Record)
    columns = [(name, dshape_to_hive(typ))
            for name, typ in zip(ds.measure.names, ds.measure.types)]
    column_text = ',\n'.join('%30s  %s' % col for col in columns)[12:]

    statement = """
        CREATE TABLE {dbname}{tblname} (
            {column_text}
            )
        ROW FORMAT FIELDS DELIMITED BY {delimiter}
                   ESCAPED BY {escapechar}
        STORED AS TEXTFILE
        LOCATION '{path}'
        """

    if csv.has_header:
        statement = statement + """
        TBLPROPERTIES ("skip.header.line.count"="1")"""

    statement = statement + ';'

    return statement.format(**locals())
