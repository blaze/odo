from __future__ import absolute_import, division, print_function

import pytest
pymongo = pytest.importorskip('pymongo')

from odo import discover, convert, append, resource, dshape, odo
from toolz import pluck
from copy import deepcopy
from bson.objectid import ObjectId


@pytest.fixture(scope='module')
def conn():
    try:
        return pymongo.MongoClient()
    except pymongo.errors.ConnectionFailure:
        pytest.skip('No mongo server running')


@pytest.yield_fixture
def db(conn):
    try:
        yield conn._test_db
    finally:
        conn.drop_database('_test_db')


@pytest.fixture
def raw_bank():
    return [
        {'name': 'Alice', 'amount': 100},
        {'name': 'Alice', 'amount': 200},
        {'name': 'Bob', 'amount': 100},
        {'name': 'Bob', 'amount': 200},
        {'name': 'Bob', 'amount': 300}
    ]


@pytest.yield_fixture
def bank(db, raw_bank):
    db.bank.insert(deepcopy(raw_bank))
    try:
        yield db.bank
    finally:
        db.drop_collection('bank')


@pytest.yield_fixture
def empty_bank(db):
    try:
        yield db.empty_bank
    finally:
        db.drop_collection('empty_bank')


ds = dshape('var * {name: string, amount: int}')


def test_discover(bank, raw_bank):
    assert discover(bank) == discover(raw_bank)


def test_discover_empty_db(db):
    # NOTE: nothing has been added to the database because our fixtures create
    # a new db and collections for each test that depends on them, so
    # 'system.indexes' won't exist here yet
    # The same is true in test_resource_db
    assert discover(db).measure.names == []


def test_discover_db(bank, db):
    assert set(discover(db).measure.names) == set(['system.indexes', 'bank'])


def test_resource_db():
    db = resource('mongodb://localhost:27017/_test_db')
    assert db.name == '_test_db'
    assert discover(db).measure.names == []


def test_resource_collection():
    coll = resource('mongodb://localhost:27017/db::mycoll')
    assert coll.name == 'mycoll'
    assert coll.database.name == 'db'
    assert coll.database.connection.host == 'localhost'
    assert coll.database.connection.port == 27017


def test_append_convert(empty_bank, raw_bank):
    ds = discover(raw_bank)
    assert set(ds.measure.names) == {'name', 'amount'}

    append(empty_bank, raw_bank, dshape=ds)
    assert odo(empty_bank, list, dshape=ds) == list(
        pluck(ds.measure.names, raw_bank)
    )


@pytest.yield_fixture
def multiple_object_ids(db):
    data = [
        {'x': 1, 'y': 2, 'other': ObjectId('1' * 24)},
        {'x': 3, 'y': 4, 'other': ObjectId('2' * 24)}
    ]
    db.multiple_object_ids.insert(data)
    try:
        yield db.multiple_object_ids
    finally:
        db.drop_collection('multiple_object_ids')


def test_multiple_object_ids(multiple_object_ids):
    assert discover(multiple_object_ids) == dshape('2 * {x: int64, y: int64}')
    assert convert(list, multiple_object_ids) == [(1, 2), (3, 4)]
