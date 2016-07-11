import time

import pytest
from datashape import dshape
from odo import odo, resource, discover, append
from odo.backends.elasticsearch import DocumentCollection
from toolz import pluck

# elasticsearch is eventually consistent
# so a small sleep is required to make the documents available.
SMALL_SLEEP_UNTIL_DOCUMENTS_AVAILABLE_FOR_QUERYING = 2

es = pytest.importorskip('elasticsearch')
elasticsearch_dsl = pytest.importorskip('elasticsearch_dsl')


@pytest.fixture(scope='module')
def elastic_host_port():
    import os
    return (os.environ.get('ELASTICSEARCH_IP', 'localhost'),
            os.environ.get('ELASTICSEARCH_PORT', 9200))


@pytest.fixture(scope='module')
def conn(elastic_host_port):
    host, port = elastic_host_port
    try:
        return es.Elasticsearch(hosts=['{}:{}'.format(host, port)])
    except es.ElasticsearchException:
        pytest.skip('No elasticsearch server running')


@pytest.yield_fixture
def doc_collection(conn):
    dc = DocumentCollection(index_name='test_', doc_type='bank', using=conn)
    try:
        dc.create()
        yield dc
    except es.ElasticsearchException:
        dc.delete_index()
        yield dc
    finally:
        dc.delete_index()


@pytest.yield_fixture
def empty_bank(conn):
    dc = DocumentCollection(index_name='test_empty_', doc_type='bank', using=conn)
    try:
        dc.create()
        yield dc
    except es.ElasticsearchException:
        dc.delete_index()
        yield dc
    finally:
        dc.delete_index()


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
def bank(doc_collection, raw_bank):
    try:
        doc_collection.bulk_insert(raw_bank)
        time.sleep(SMALL_SLEEP_UNTIL_DOCUMENTS_AVAILABLE_FOR_QUERYING)
        yield doc_collection
    finally:
        doc_collection.delete()
        time.sleep(SMALL_SLEEP_UNTIL_DOCUMENTS_AVAILABLE_FOR_QUERYING)


ds = dshape('var * {name: string, amount: int}')


def test_resource_collection(elastic_host_port):
    host, port = elastic_host_port
    coll = resource('elasticsearch://{}:{}/index::test_docs'.format(host, port))
    assert coll._doc_type == 'test_docs'
    elastic_host = coll.connection.transport.hosts[0]
    assert elastic_host['host'] == host
    assert elastic_host['port'] == port


def test_discover(bank, raw_bank):
    assert discover(bank) == discover(raw_bank)


def test_append_convert(empty_bank, raw_bank):
    ds = discover(raw_bank)
    assert set(ds.measure.names) == {'name', 'amount'}

    append(empty_bank, raw_bank, dshape=ds)
    time.sleep(SMALL_SLEEP_UNTIL_DOCUMENTS_AVAILABLE_FOR_QUERYING)
    assert set(odo(empty_bank, list, dshape=ds)) == set(list(
        pluck(ds.measure.names, raw_bank)
    ))
