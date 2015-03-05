from __future__ import absolute_import, division, print_function

import gzip
from datashape import discover, dshape
from collections import Iterator
from toolz import partial, concat
import uuid
import os

from ..compatibility import unicode
from ..chunks import chunks
from ..drop import drop
from ..temp import Temp
from ..append import append
from ..convert import convert
from ..resource import resource


class TextFile(object):
    canonical_extension = 'txt'

    def __init__(self, path, **kwargs):
        self.path = path

    @property
    def open(self):
        if self.path.split(os.path.extsep)[-1] == 'gz':
            return gzip.open
        else:
            return open


@convert.register(Iterator, (TextFile, Temp(TextFile)), cost=0.1)
def textfile_to_iterator(data, **kwargs):
    with data.open(data.path) as f:
        for line in f:
            yield line


@convert.register(Iterator, chunks(TextFile), cost=0.1)
def chunks_textfile_to_iterator(data, **kwargs):
    return concat(map(partial(convert, Iterator), data))


@discover.register((TextFile, Temp(TextFile)))
def discover_textfile(data, **kwargs):
    return dshape('var * string')


@append.register((Temp(TextFile), TextFile), Iterator)
def append_iterator_to_textfile(target, source, **kwargs):
    with target.open(target.path, 'a') as f:
        for item in source:
            f.write(unicode(item))
            f.write('\n')  # TODO: detect OS-level newline character
    return target


@append.register(TextFile, object)
def append_anything_to_textfile(target, source, **kwargs):
    return append(target, convert(Iterator, source, **kwargs), **kwargs)


@convert.register(Temp(TextFile), Iterator)
def iterator_to_temp_textfile(seq, **kwargs):
    fn = str(uuid.uuid1())
    txt = Temp(TextFile)(fn)
    return append(txt, seq, **kwargs)


@resource.register('.+\.(txt|log)(.gz)?')
def resource_sas(uri, **kwargs):
    return TextFile(uri)


@drop.register(TextFile)
def drop_textfile(data, **kwargs):
    os.remove(data.path)
