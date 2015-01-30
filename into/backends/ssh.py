from __future__ import absolute_import, division, print_function

import paramiko
from contextlib import contextmanager
from toolz import keyfilter, memoize, take
from datashape import discover
import re
import uuid

from ..directory import _Directory, Directory
from ..utils import keywords, tmpfile, sample
from ..resource import resource
from ..append import append
from ..convert import convert
from ..temp import Temp
from ..drop import drop
from .csv import CSV
from .json import JSON, JSONLines

@contextmanager
def connect(**auth):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(**auth)

    try:
        yield ssh
    finally:
        ssh.close()


@contextmanager
def sftp(**auth):
    with connect(**auth) as ssh:
        sftp = ssh.open_sftp()

        yield sftp


class _SSH(object):
    """ Parent class for data accessed through ``ssh``

    See ``paramiko.SSHClient.connect`` for authentication keyword arguments

    Examples
    --------

    >>> from into import SSH, CSV
    >>> s = SSH(CSV)('/path/to/file.csv', hostname='hostname', username='alice')

    Normally create through resource uris

    >>> data = resource('ssh://alice@host:/path/to/file.csv', password='pass')
    >>> data.path
    '/path/to/file.csv'
    >>> data.auth['hostname']
    'host'
    """
    def __init__(self, *args, **kwargs):
        self.auth = keyfilter(keywords(paramiko.SSHClient.connect).__contains__,
                              kwargs)
        self.subtype.__init__(self, *args, **kwargs)

    def lines(self):
        with sftp(**self.auth) as conn:
            for line in conn.file(self.path, 'r'):
                yield line


def SSH(cls):
    return type('SSH(%s)' % cls.__name__, (_SSH, cls), {'subtype':  cls})

SSH.__doc__ = _SSH.__doc__

SSH = memoize(SSH)


types_by_extension = {'csv': CSV, 'json': JSONLines}

ssh_pattern = '((?P<username>[a-zA-Z]\w*)@)?(?P<hostname>[\w.-]*)(:(?P<port>\d+))?:(?P<path>[/\w.*-]+)'

@resource.register('ssh://.+', priority=16)
def resource_ssh(uri, **kwargs):
    if 'ssh://' in uri:
        uri = uri[len('ssh://'):]

    d = re.match(ssh_pattern, uri).groupdict()
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

    return SSH(subtype)(path, **kwargs)


@sample.register((SSH(CSV),
                  SSH(JSON),
                  SSH(JSONLines)))
@contextmanager
def sample_ssh(data, lines=500):
    """ Grab a few lines from the remote file """
    with tmpfile() as fn:
        with open(fn, 'w') as f:
            for line in take(lines, data.lines()):
                f.write(line)
                f.write('\n')
        yield fn


@sample.register((SSH(Directory(CSV)),
                  SSH(Directory(JSON)),
                  SSH(Directory(JSONLines))))
@contextmanager
def sample_ssh(data, **kwargs):
    """ Grab a few lines from a file in a remote directory """
    with sftp(**data.auth) as conn:
        fn = data.path + '/' + conn.listdir(data.path)[0]
        one_file = SSH(data.container)(fn, **data.auth)
        with sample(one_file, **kwargs) as result:
            yield result


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


@discover.register((SSH(JSON), SSH(JSONLines)))
def discover_ssh_json(data, **kwargs):
    with sample(data) as fn:
        result = discover(data.subtype(fn))
    return result


@discover.register((SSH(Directory(CSV)),
                    SSH(Directory(JSON)),
                    SSH(Directory(JSONLines))))
def discover_ssh_directory(data, **kwargs):
    with sftp(**data.auth) as conn:
        fn = data.path + '/' + conn.listdir(data.path)[0]
        one_file = SSH(data.container)(fn, **data.auth)
        result = discover(one_file)
    return result


@drop.register((_SSH, SSH(CSV), SSH(JSON), SSH(JSONLines)))
def drop_ssh(data, **kwargs):
    with sftp(**data.auth) as conn:
        conn.remove(data.path)


@append.register(_SSH, object)
def append_anything_to_ssh(target, source, **kwargs):
    if not isinstance(source, target.subtype):
        source = convert(Temp(target.subtype), source, **kwargs)
    # TODO: handle overwrite case
    with sftp(**target.auth) as conn:
        conn.put(source.path, target.path)
    return target


@append.register(JSONLines, SSH(JSONLines))
@append.register(JSON, SSH(JSON))
@append.register(CSV, SSH(CSV))
def append_sshX_to_X(target, source, **kwargs):
    # TODO: handle overwrite case
    with sftp(**source.auth) as conn:
        conn.get(source.path, target.path)
    return target


@convert.register(Temp(SSH(JSONLines)), (Temp(JSONLines), JSONLines))
def csv_to_temp_ssh_jsonlines(data, **kwargs):
    fn = '.%s.json' % uuid.uuid1()
    target = Temp(SSH(JSONLines))(fn, **kwargs)
    return append(target, data, **kwargs)

@convert.register(Temp(SSH(JSON)), (Temp(JSON), JSON))
def csv_to_temp_ssh_json(data, **kwargs):
    fn = '.%s.json' % uuid.uuid1()
    target = Temp(SSH(JSON))(fn, **kwargs)
    return append(target, data, **kwargs)

@convert.register(Temp(SSH(CSV)), (Temp(CSV), CSV))
def csv_to_temp_ssh_csv(data, **kwargs):
    fn = '.%s.csv' % uuid.uuid1()
    target = Temp(SSH(CSV))(fn, **kwargs)
    return append(target, data, **kwargs)


@convert.register(Temp(JSON), (Temp(SSH(JSON)), SSH(JSON)))
@convert.register(Temp(JSONLines), (Temp(SSH(JSONLines)), SSH(JSONLines)))
@convert.register(Temp(CSV), (Temp(SSH(CSV)), SSH(CSV)))
def ssh_csv_to_temp_csv(data, **kwargs):
    fn = '.%s' % uuid.uuid1()
    target = Temp(data.subtype)(fn, **kwargs)
    return append(target, data, **kwargs)
