import os
import itertools

import pytest

from kdbpy import kdb as k
from kdbpy.exampleutils import example_data

syms = itertools.count()


@pytest.fixture
def gensym():
    return 'sym%d' % next(syms)


@pytest.fixture(scope='session')
def creds():
    # must be module scoped because of downstream usage
    return k.default_credentials


@pytest.yield_fixture(scope='module')
def kq(creds):
    with k.KQ(creds, start=True) as r:
        r.load_libs()
        yield r


@pytest.fixture(scope='module')
def kdb(kq):
    kq.read_kdb(os.path.join(os.path.dirname(__file__), 'conftest.q'))
    return kq


@pytest.fixture(scope='module')
def kdbpar(kq):
    path = example_data(os.path.join('start', 'db'))
    assert os.path.exists(path)
    kq.read_kdb(path)
    return kq


@pytest.fixture
def df(kdb):
    return kdb.eval('t')


@pytest.fixture
def date_df(kdb):
    return kdb.eval('date_t')


@pytest.fixture
def rdf(kdb):
    return kdb.eval('rt')


@pytest.fixture
def sdf(kdb):
    return kdb.eval('st')


@pytest.fixture(scope='session')
def rstring(creds):
    return 'kdb://%s@%s:%d' % (creds.username, creds.host, creds.port)
