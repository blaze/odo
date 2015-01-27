from __future__ import absolute_import, division, print_function

import paramiko
from contextlib import contextmanager
from toolz import keyfilter, memoize, take
from datashape import discover
import re

from ..utils import keywords, tmpfile
from ..resource import resource


class _SSH(object):
    """ Parent class for data accessed through ssh

    See ``paramiko.SSHClient.connect`` for authentication keyword arguments

    Examples
    --------

    >>> from into import SSH, CSV
    >>> SSH(CSV)('/path/to/file.csv')

    Normally create through resource uris

    >>> data = resource('ssh://alice@hostname:/path/to/file.csv')
    >>> data.path
    '/path/to/file.csv'
    >>> data.hostname
    'hostname'
    """
    def __init__(self, *args, **kwargs):
        self.auth = keyfilter(keywords(paramiko.SSHClient.connect).__contains__,
                              kwargs)
        self.subtype.__init__(self, *args, **kwargs)

    @memoize
    def connect(self):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(**self.auth)
        return ssh

    def lines(self):
        ssh = self.connect()
        sftp = ssh.open_sftp()
        return sftp.file(self.path, 'r')


def SSH(cls):
    return type('SSH(%s)' % cls.__name__, (_SSH, cls), {'subtype':  cls})

SSH.__doc__ = _SSH.__doc__

SSH = memoize(SSH)


from .csv import CSV
from .json import JSONLines
types_by_extension = {'csv': CSV, 'json': JSONLines}


@resource.register('ssh://.+', priority=14)
def resource_ssh(uri, **kwargs):
    if 'ssh://' in uri:
        uri = uri[len('ssh://'):]

    pattern = '((?P<username>[a-zA-Z]\w*)@)?(?P<hostname>[a-zA-Z][\w.-]*)(:(?P<port>\d+))?:(?P<path>[/\w.-]+)'
    d = re.match(pattern, uri).groupdict()
    path = d.pop('path')

    kwargs.update(d)

    try:
        subtype = types_by_extension[path.split('.')[-1]]
    except KeyError:
        subtype = type(resource(path))

    return SSH(subtype)(path, **kwargs)


@contextmanager
def sample(data, lines=500):
    """ Grab a few lines from the remote file """
    with tmpfile() as fn:
        with open(fn, 'w') as f:
            for line in take(lines, data.lines()):
                f.write(line)
                f.write('\n')
        yield fn


@discover.register(_SSH)
def discover_ssh(data, **kwargs):
    with sample(data) as fn:
        o = data.subtype(fn)
        result = discover(o)
    return result


@discover.register(SSH(CSV))
def discover_ssh_csv(data, **kwargs):
    with sample(data) as fn:
        o = CSV(fn, encoding=data.encoding, has_header=data.has_header, **data.dialect)
        result = discover(o)
    return result
