from __future__ import absolute_import, division, print_function

import json
import re
from collections import Iterator

import elasticsearch
from datashape import Record, var, isdimension
from datashape.discovery import discover
from datashape.dispatch import dispatch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Search
from elasticsearch_dsl.query import MatchAll
from odo import convert
from odo.core import ooc_types
from toolz import concat, pluck, take

from ..append import append
from ..resource import resource


class DocumentCollection(Index):
    """
    Parametrized ElasticSearch Index
    """

    def __init__(self, index_name, doc_type, using='default'):
        super(DocumentCollection, self).__init__(name=index_name, using=using)
        self._doc_type = doc_type

    def search(self):
        return Search(
            using=self._using,
            index=self._name,
            doc_type=self._doc_type
        )

    def delete(self, **kwargs):
        """
        Delete implementation for ES2
        Similar to https://www.elastic.co/guide/en/elasticsearch/plugins/current/delete-by-query-usage.html
        
        """
        doctype_ids = (document.meta.id for document in self.search().fields([]).scan())
        helpers.bulk(self.connection,
                     ({
                          '_op_type': 'delete',
                          '_index': self._name,
                          '_type': self._doc_type,
                          '_id': i,
                      } for i in doctype_ids),
                     chunk_size=1024)

    def delete_index(self, **kwargs):
        super(DocumentCollection, self).delete(**kwargs)

    def __repr__(self):
        host = self.connection.transport.hosts[0]
        return '<DocumentCollection on {host}:{port} - {index}/{doctype}>'.format(
            host=host['host'], port=host['port'], index=self._doc_type, doctype=self._name)

    def bulk_insert(self, documents_iter, chunksize=1024):
        bulk_iter = ({'_type': self._doc_type, '_index': self._name, '_source': json.dumps(d)} for d in documents_iter)
        helpers.bulk(self.connection, bulk_iter, chunk_size=chunksize)

    def head(self, n):
        """
        Returns the first n docuemnts in this collection.

        Parameters
        ----------
        n: int
            Number of results to fetch

        Returns
        -------
        List of results
        """
        return list(take(n, iter(self)))

    def __iter__(self):
        return self.search().query(MatchAll()).scan()


@discover.register(DocumentCollection)
def discover_elasticsearch_index(collection, n=10):
    """
    Parameters
    ----------
    collection : DocumentCollection
    n: int
        Number of results to check for shape consistency
    """
    items = collection.head(n)
    items = list(map(es_result_to_dict, items))

    if not items:
        return var * Record([])

    ds = discover(items)
    if isdimension(ds[0]):
        return collection.search().count() * ds.subshape[0]
    else:
        raise ValueError("Consistent datashape not found")


@convert.register(Iterator, DocumentCollection, cost=500.0)
def index_to_iterator(collection, columns=None, dshape=None, **kwargs):
    seq = map(es_result_to_dict, collection)
    if not columns and dshape:
        columns = dshape.measure.names
    elif not columns:
        item = next(seq)
        seq = concat([[item], seq])
        columns = sorted(item.keys())

    return pluck(columns, seq)


def es_result_to_dict(result):
    return result.__dict__['_d_']


@append.register(DocumentCollection, Iterator)
def append_seq_to_index(collection, seq, columns=None, dshape=None, chunksize=1024, **kwargs):
    seq = iter(seq)
    peek = next(seq)

    # reconstruct
    seq = concat([[peek], seq])

    if isinstance(peek, (tuple, list)):
        if not columns and dshape:
            columns = dshape.measure.names
        if not columns:
            raise ValueError("Inputs must be dictionaries. "
                             "Or provide columns=[...] or dshape=DataShape(...) keyword")
        seq = (dict(zip(columns, item)) for item in seq)

    collection.bulk_insert(seq, chunksize=chunksize)

    return collection


@append.register(DocumentCollection, object)
def append_anything_to_index(index, o, **kwargs):
    return append(index, convert(Iterator, o, **kwargs), **kwargs)


@resource.register(r'elasticsearch://\w*:\w*@\w*.*', priority=11)
def resource_elasticsearch_with_authentication(uri, doctype=None, **kwargs):
    pattern = r'elasticsearch://(?P<user>\w*):(?P<pass>\w*)@(?P<hostport>.*:?\d*)/(?P<index>\w+)'
    d = re.search(pattern, uri).groupdict()
    return _resource_elasticsearch(d, doctype)


@resource.register(r'elasticsearch://.+')
def resource_elasticsearch(uri, doctype=None, **kwargs):
    pattern = r'elasticsearch://(?P<hostport>.*:?\d*)/(?P<index>\w+)'
    d = re.search(pattern, uri).groupdict()
    return _resource_elasticsearch(d, doctype)


def _resource_elasticsearch(conn_info_dict, doctype):
    auth_info = None
    if conn_info_dict.get('user'):  # need to authenticate
        auth_info = (conn_info_dict['user'], conn_info_dict['pass'])

    client = elasticsearch.Elasticsearch(hosts=[conn_info_dict['hostport']], http_auth=auth_info)

    # noinspection PyTypeChecker
    collection = DocumentCollection(conn_info_dict.get('index'), doc_type=doctype, using=client)

    return collection


ooc_types.add(DocumentCollection)


@dispatch(DocumentCollection)
def drop(i):
    i.delete()
