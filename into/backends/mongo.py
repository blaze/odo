from __future__ import absolute_import, division, print_function

import copy
from collections import Iterator
from datashape import discover
from pymongo.collection import Collection
from datashape.predicates import isdimension
from toolz import take, partition_all, concat, pluck
from ..convert import convert
from ..append import append
from ..resource import resource


@discover.register(Collection)
def discover_pymongo_collection(coll, n=50):
    items = list(take(n, coll.find()))
    for item in items:
        del item['_id']

    ds = discover(items)

    if isdimension(ds[0]):
        return coll.count() * ds.subshape[0]
    else:
        raise ValueError("Consistent datashape not found")


def _into_iter_mongodb(coll, columns=None, dshape=None):
    """ Into helper function

    Return both a lazy sequence of tuples and a list of column names
    """
    seq = coll.find()
    if not columns and dshape:
        columns = dshape.measure.names
    elif not columns:
        item = next(seq)
        seq = concat([[item], seq])
        columns = sorted(item.keys())
        columns.remove('_id')
    return columns, pluck(columns, seq)


@convert.register(Iterator, Collection)
def collection_to_iterator(coll, columns=None, dshape=None, **kwargs):
    columns, seq = _into_iter_mongodb(coll, columns=columns, dshape=dshape)
    return seq


@append.register(Collection, Iterator)
def append_iterator_to_pymongo(coll, seq, columns=None, dshape=None, chunksize=1024, **kwargs):
    seq = iter(seq)
    item = next(seq)
    seq = concat([[item], seq])

    if isinstance(item, (tuple, list)):
        if not columns and dshape:
            columns = dshape.measure.names
        if not columns:
            raise ValueError("Inputs must be dictionaries. "
                "Or provide columns=[...] or dshape=DataShape(...) keyword")
        seq = (dict(zip(columns, item)) for item in seq)

    for block in partition_all(1024, seq):
        coll.insert(copy.deepcopy(block))

    return coll


@append.register(Collection, object)
def append_anything_to_collection(coll, o, **kwargs):
    return append(coll, convert(Iterator, o, **kwargs), **kwargs)
