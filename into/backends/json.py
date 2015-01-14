from __future__ import absolute_import, division, print_function

import json
from toolz.curried import map, take, pipe, pluck, get
from collections import Iterator
import os

from datashape import discover, var, dshape, Record
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..chunks import chunks
from ..numpy_dtype import dshape_to_pandas
from .pandas import coerce_datetimes


class JSON(object):
    """ Proxy for a JSON file

    Parameters
    ----------

    path : str
        Path to file on disk

    See Also
    --------

    JSONLines - Line-delimited JSON
    """
    def __init__(self, path):
        self.path = path


class JSONLines(object):
    """ Proxy for a line-delimited JSON file

    Each line in the file is a valid JSON entry

    Parameters
    ----------

    path : str
        Path to file on disk

    See Also
    --------

    JSON - Not-line-delimited JSON
    """
    def __init__(self, path):
        self.path = path


@discover.register(JSON)
def discover_json(j, **kwargs):
    with open(j.path) as f:
        data = json.load(f)
    return discover(data)


@discover.register(JSONLines)
def discover_json(j, n=10, **kwargs):
    with open(j.path) as f:
        data = pipe(f, map(json.loads), take(n), list)
    if len(data) < n:
        return discover(data)
    else:
        return datashape.var * discover(data).subshape[0]


@convert.register(list, JSON)
def json_to_list(j, dshape=None, **kwargs):
    with open(j.path) as f:
        data = json.load(f)
    return data


@convert.register(Iterator, JSONLines)
def json_lines_to_iterator(j, **kwargs):
    f = open(j.path)
    return map(json.loads, f)


@append.register(JSONLines, object)
def object_to_jsonlines(j, o, **kwargs):
    return append(j, convert(Iterator, o, **kwargs), **kwargs)


@append.register(JSONLines, Iterator)
def iterator_to_json_lines(j, seq, **kwargs):
    with open(j.path, 'a') as f:
        for item in seq:
            json.dump(item, f)
            f.write('\n')
    return j


@append.register(JSON, list)
def list_to_json(j, seq, **kwargs):
    if os.path.exists(j.path):
        with open(j.path) as f:
            assert not json.load(f)  # assert empty

    with open(j.path, 'w') as f:
        json.dump(seq, f)


@append.register(JSON, object)
def object_to_json(j, o, **kwargs):
    return append(j, convert(list, o, **kwargs), **kwargs)


@resource.register('json://.*\.json', priority=11)
def resource_json(path, **kwargs):
    if 'json://' in path:
        path = path[len('json://'):]
    return JSON(path)


@resource.register('jsonlines://.*\.json', priority=11)
def resource_jsonlines(path, **kwargs):
    if 'jsonlines://' in path:
        path = path[len('jsonlines://'):]
    return JSONLines(path)


@resource.register('.*\.json')
def resource_json_ambiguous(path, **kwargs):
    """ Try to guess if this file is line-delimited or not """
    if os.path.exists(path):
        with open(path) as f:
            one = next(f)
            try:
                two = next(f)
            except StopIteration:  # only one line
                return resource_json(path, **kwargs)
            try:
                json.loads(one)
                return resource_jsonlines(path, **kwargs)
            except:
                return resource_json(path, **kwargs)

    # File doesn't exist, is the dshape variable length?
    dshape = kwargs.get('dshape', None)
    if not dshape:
        raise ValueError("Don't know if you intend line-delimited or no."
                "Please add protocol json:// or jsonlines:// as appropriate")
    if dshape[0] == var:
        return resource_jsonlines(path, dshape=dshape, **kwargs)
    else:
        return resource_json(path, dshape=dshape, **kwargs)


ooc_types.add(JSONLines)
