from toolz import memoize
from functools import wraps

from .csv import CSV
from datashape import discover
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
