from __future__ import absolute_import, division, print_function

import pytest
pytest.importorskip('pymongo')

import pymongo
from contextlib import contextmanager
import datashape
from into import discover, convert, append, resource, dshape
from into.backends.mongo import *
from toolz import pluck
from copy import deepcopy
from bson.objectid import ObjectId


try:
    conn = pymongo.MongoClient()
except pymongo.errors.ConnectionFailure:
    pytest.skip("unable to connect to mongo")

db = conn._test_db


@contextmanager
def coll(data):
    c = db.my_collection
    if data:
        c.insert(deepcopy(data))

    try:
        yield c
    finally:
        c.drop()

@pytest.fixture(scope='module')
def ds():
    return dshape('var * {name: string, amount: int}')

def test_discover(bank):
    with coll(bank) as c:
        assert discover(bank) == discover(c)


def test_append_convert(bank, ds):
    with coll([]) as c:
        append(c, bank, dshape=ds)

        assert convert(list, c, dshape=ds) == list(pluck(['name', 'amount'], bank))


def test_resource():
    coll = resource('mongodb://localhost:27017/db::mycoll')
    assert coll.name == 'mycoll'
    assert coll.database.name == 'db'
    assert coll.database.connection.host == 'localhost'
    assert coll.database.connection.port == 27017


def test_multiple_object_ids():
    data = [{'x': 1, 'y': 2, 'other': ObjectId('1' * 24)},
            {'x': 3, 'y': 4, 'other': ObjectId('2' * 24)}]
    with coll(data) as c:
        assert discover(c) == dshape('2 * {x: int64, y: int64}')

        assert convert(list, c) == [(1, 2), (3, 4)]
