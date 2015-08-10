from __future__ import absolute_import, division, print_function

import paramiko
from contextlib import contextmanager
from toolz import keyfilter, memoize, take, curry
from datashape import discover
import re
import uuid

from ..directory import Directory
from ..utils import keywords, tmpfile, sample, ignoring, copydoc
from ..resource import resource
from ..append import append
from ..convert import convert
from ..temp import Temp, _Temp
from ..drop import drop
from .csv import CSV
from .json import JSON, JSONLines
from .text import TextFile

connection_pool = dict()

def connect(**auth):
    key = tuple(sorted(auth.items()))
    if key in connection_pool:
        ssh = connection_pool[key]
        if not ssh.get_transport() or not ssh.get_transport().is_active():
            ssh.connect(**auth)
    else:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(**auth)
        connection_pool[key] = ssh
    return ssh

sftp_pool = dict()

def sftp(**auth):
    ssh = connect(**auth)  # Need to call this explicitly (can't memoize)
    key = tuple(sorted(auth.items()))
    if key in sftp_pool:
        conn = sftp_pool[key]
    else:
        conn = ssh.open_sftp()
        sftp_pool[key] = conn

    conn.sock.setblocking(True)

    return conn


class _SSH(object):
    """ Parent class for data accessed through ``ssh``

    See ``paramiko.SSHClient.connect`` for authentication keyword arguments

    Examples
    --------

    >>> from odo import SSH, CSV
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
        conn = sftp(**self.auth)
        return conn.file(self.path, 'r')


@memoize
@copydoc(_SSH)
def SSH(cls):
    return type('SSH(%s)' % cls.__name__, (_SSH, cls), {'subtype':  cls})


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
        yield fn


@sample.register((SSH(Directory(CSV)),
                  SSH(Directory(JSON)),
                  SSH(Directory(JSONLines))))
@contextmanager
def sample_ssh(data, **kwargs):
    """ Grab a few lines from a file in a remote directory """
    conn = sftp(**data.auth)
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
    conn = sftp(**data.auth)
    fn = data.path + '/' + conn.listdir(data.path)[0]
    one_file = SSH(data.container)(fn, **data.auth)
    result = discover(one_file)
    return result


@drop.register((_SSH, SSH(CSV), SSH(JSON), SSH(JSONLines)))
def drop_ssh(data, **kwargs):
    conn = sftp(**data.auth)
    with ignoring(IOError):
        conn.remove(data.path)


@append.register(_SSH, object)
def append_anything_to_ssh(target, source, **kwargs):
    if not isinstance(source, target.subtype):
        source = convert(Temp(target.subtype), source, **kwargs)
    # TODO: handle overwrite case
    conn = sftp(**target.auth)
    conn.put(source.path, target.path)
    return target


@append.register(TextFile, SSH(TextFile))
@append.register(JSONLines, SSH(JSONLines))
@append.register(JSON, SSH(JSON))
@append.register(CSV, SSH(CSV))
def append_sshX_to_X(target, source, **kwargs):
    # TODO: handle overwrite case
    conn = sftp(**source.auth)
    conn.get(source.path, target.path)
    return target


@curry
def file_to_temp_ssh_file(typ, data, **kwargs):
    """ Generic convert function sending data to ssh(data)

    Needs to be partially evaluated with a type"""
    # don't use . prefix to hide because Hive doesn't like it
    fn = '%s.%s' % (uuid.uuid1(), typ.canonical_extension)
    target = Temp(SSH(typ))(fn, **kwargs)
    return append(target, data, **kwargs)

for typ in [CSV, JSON, JSONLines, TextFile]:
    convert.register(Temp(SSH(typ)), (Temp(typ), typ))(
            file_to_temp_ssh_file(typ))


@convert.register(Temp(TextFile), (Temp(SSH(TextFile)), SSH(TextFile)))
@convert.register(Temp(JSONLines), (Temp(SSH(JSONLines)), SSH(JSONLines)))
@convert.register(Temp(JSON), (Temp(SSH(JSON)), SSH(JSON)))
@convert.register(Temp(CSV), (Temp(SSH(CSV)), SSH(CSV)))
def ssh_file_to_temp_file(data, **kwargs):
    fn = '.%s' % uuid.uuid1()
    target = Temp(data.subtype)(fn, **kwargs)
    return append(target, data, **kwargs)


@convert.register(Temp(SSH(TextFile)), (TextFile, Temp(TextFile)))
@convert.register(Temp(SSH(JSONLines)), (JSONLines, Temp(JSONLines)))
@convert.register(Temp(SSH(JSON)), (JSON, Temp(JSON)))
@convert.register(Temp(SSH(CSV)), (CSV, Temp(CSV)))
def file_to_temp_ssh_file(data, **kwargs):
    fn = '%s' % uuid.uuid1()
    if isinstance(data, _Temp):
        target = Temp(SSH(data.persistent_type))(fn, **kwargs)
    else:
        target = Temp(SSH(type(data)))(fn, **kwargs)
    return append(target, data, **kwargs)
