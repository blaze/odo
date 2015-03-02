from __future__ import absolute_import, division, print_function

from ..resource import resource
from ..drop import drop
import os
import datashape
from multipledispatch import MDNotImplementedError


class File(object):
    def __init__(self, path, **kwargs):
        self.path = path


@resource.register('.*', priority=8)
def resource_file(uri, **kwargs):
    if '://' in uri or '@' in uri:
        raise MDNotImplementedError()
    return File(uri)


@drop.register(File)
def drop_file(data, **kwargs):
    os.remove(data.path)


@datashape.discover.register(File)
def discover_file(data, **kwargs):
    return datashape.var * datashape.bytes_
