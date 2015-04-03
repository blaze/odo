from __future__ import absolute_import, division, print_function

import pymongo
from pymongo.collection import Collection
from collections import Iterator
from datashape import discover, DataShape, Record, var
from datashape.predicates import isdimension
from datashape.dispatch import dispatch
from toolz import take, partition_all, concat, pluck
import copy
from bson.objectid import ObjectId
import re
from ..convert import convert, ooc_types
from ..append import append
from ..resource import resource


@discover.register(Collection)
def discover_pymongo_collection(coll, n=50):
    items = list(take(n, coll.find()))
    if not items:
        return var * Record([])
    oid_cols = [k for k, v in items[0].items() if isinstance(v, ObjectId)]
    for item in items:
        for col in oid_cols:
            del item[col]

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


@convert.register(Iterator, Collection, cost=500.0)
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


@resource.register(r'mongodb://\w*:\w*@\w*.*', priority=11)
def resource_mongo_with_authentication(uri, collection_name=None, **kwargs):
    pattern = r'mongodb://(?P<user>\w*):(?P<pass>\w*)@(?P<hostport>.*:?\d*)/(?P<database>\w+)'
    d = re.search(pattern, uri).groupdict()
    return _resource_mongo(d, collection_name)


@resource.register(r'mongodb://.+')
def resource_mongo(uri, collection_name=None, **kwargs):
    pattern = r'mongodb://(?P<hostport>.*:?\d*)/(?P<database>\w+)'
    d = re.search(pattern, uri).groupdict()
    return _resource_mongo(d, collection_name)


def _resource_mongo(d, collection_name=None):
    client = pymongo.MongoClient(d['hostport'])
    db = getattr(client, d['database'])
    if d.get('user'):
        db.authenticate(d['user'], d['pass'])
    if collection_name is None:
        return db
    return getattr(db, collection_name)


@discover.register(pymongo.database.Database)
def discover_mongo_database(db):
    names = db.collection_names()
    return DataShape(Record(zip(names, (discover(getattr(db, name))
                                        for name in names))))


ooc_types.add(Collection)


@dispatch(Collection)
def drop(m):
    m.drop()
