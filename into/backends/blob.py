from __future__ import absolute_import, division, print_function

from ..resource import resource
from ..convert import convert, ooc_types
from ..append import append
from ..drop import drop
import os
import shutil
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


@append.register(File, File)
def append_file_to_file(tgt, src, **kwargs):
    shutil.copy(src.path, tgt.path)
    return tgt


@append.register(File, object)
def append_file_to_file(tgt, src, **kwargs):
    return append(tgt, convert(File, src, **kwargs), **kwargs)


ooc_types.add(File)
