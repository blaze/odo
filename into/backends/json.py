from __future__ import absolute_import, division, print_function

import json
from toolz.curried import map, take, pipe, pluck, get, concat
from collections import Iterator, Iterable
import os

from datashape import discover, var, dshape, Record, DataShape
from datashape import coretypes as ct
from datashape.dispatch import dispatch
import datetime
from ..append import append
from ..convert import convert, ooc_types
from ..resource import resource
from ..utils import tuples_to_records


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


def date_to_datetime_dshape(ds):
    shape = ds.shape
    if isinstance(ds.measure, Record):
        measure = Record([[name, ct.datetime_ if typ == ct.date_ else typ]
            for name, typ in ds.measure.parameters[0]])
    else:
        measure = ds.measure
    return DataShape(*(shape + (measure,)))


@discover.register(JSON)
def discover_json(j, **kwargs):
    with open(j.path) as f:
        data = json.load(f)
    ds = discover(data)
    return date_to_datetime_dshape(ds)


@discover.register(JSONLines)
def discover_json(j, n=10, **kwargs):
    with open(j.path) as f:
        data = pipe(f, map(json.loads), take(n), list)
    if len(data) < n:
        ds = discover(data)
    else:
        ds = datashape.var * discover(data).subshape[0]
    return date_to_datetime_dshape(ds)



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
def iterator_to_json_lines(j, seq, dshape=None, **kwargs):
    row = next(seq)
    seq = concat([[row], seq])
    if not isinstance(row, (dict, str)) and isinstance(row, Iterable):
        seq = tuples_to_records(dshape, seq)
    with open(j.path, 'a') as f:
        for item in seq:
            json.dump(item, f, default=json_dumps)
            f.write('\n')
    return j


@append.register(JSON, list)
def list_to_json(j, seq, dshape=None, **kwargs):
    if not isinstance(seq[0], (dict, str)) and isinstance(seq[0], Iterable):
        seq = list(tuples_to_records(dshape, seq))
    if os.path.exists(j.path):
        with open(j.path) as f:
            assert not json.load(f)  # assert empty

    with open(j.path, 'w') as f:
        json.dump(seq, f, default=json_dumps)
    return j


@append.register(JSON, object)
def object_to_json(j, o, **kwargs):
    return append(j, convert(list, o, **kwargs), **kwargs)


@resource.register('json://.*\.json', priority=11)
def resource_json(path, **kwargs):
    if 'json://' in path:
        path = path[len('json://'):]
    return JSON(path)


@resource.register('.*\.jsonlines', priority=11)
@resource.register('jsonlines://.*\.json', priority=11)
def resource_jsonlines(path, **kwargs):
    if 'jsonlines://' in path:
        path = path[len('jsonlines://'):]
    return JSONLines(path)


@resource.register('.*\.json')
def resource_json_ambiguous(path, **kwargs):
    """ Try to guess if this file is line-delimited or not """
    if os.path.exists(path):
        f = open(path)
        one = next(f)
        try:
            two = next(f)
        except StopIteration:  # only one line
            f.close()
            return resource_json(path, **kwargs)
        try:
            json.loads(one)
            f.close()
            return resource_jsonlines(path, **kwargs)
        except:
            f.close()
            return resource_json(path, **kwargs)

    # File doesn't exist, is the dshape variable length?
    dshape = kwargs.get('expected_dshape', None)
    if dshape and dshape[0] == var:
        return resource_jsonlines(path, **kwargs)
    else:
        return resource_json(path, **kwargs)


@dispatch(datetime.datetime)
def json_dumps(dt):
    s = dt.isoformat()
    if not dt.tzname():
        s = s + 'Z'
    return s


@dispatch(datetime.date)
def json_dumps(dt):
    return dt.isoformat()


ooc_types.add(JSONLines)
