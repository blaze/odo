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
    # must be module scoped because of downstream
    return k.default_credentials


@pytest.yield_fixture(scope='module')
def kq(creds):
    with k.KQ(creds, start=True) as r:
        yield r


@pytest.fixture(scope='module')
def kdb(kq):
    kq.eval('n: 6; t: ([] '
            'name: `Bob`Alice`Joe`Smithers`Smithers`Alice;'
            'id: til n;'
            'amount: n ? 10.0;'
            'when: 2003.12.05D22:23:12 '
            '2005.03.04D03:24:45.514 '
            '2005.02.28D01:58:04.338 '
            '2004.01.25D18:03:47.234 '
            '2005.02.28D01:58:04.338 '
            '2004.01.25D18:03:47.234;'
            'on: 2010.01.01 + til n)')
    kq.eval('rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2; '
            'street: `maple`apple`pine`grove)')
    kq.eval('st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; '
            'tree: `maple`apple`pine; alias: `Joe`Betty`Moe)')
    kq.eval('kt: ([house: `a`b`c; id: 1 2 3] amount: 3.0 4.0 5.0)')
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
def rdf(kdb):
    return kdb.eval('rt')


@pytest.fixture
def sdf(kdb):
    return kdb.eval('st')
