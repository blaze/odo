import os
import itertools

import pytest

from .kdb import KQ, get_credentials
from kdbpy.exampleutils import example_data

syms = itertools.count()


@pytest.fixture
def gensym():
    return 'sym%d' % next(syms)


@pytest.yield_fixture(scope='module')
def kdb():
    r = KQ(get_credentials(), start='restart')
    r.eval('n: 6; t: ([] '
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
    r.eval('rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2; '
           'street: `maple`apple`pine`grove)')
    r.eval('st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; '
           'tree: `maple`apple`pine; alias: `Joe`Betty`Moe)')
    yield r
    r.stop()


@pytest.yield_fixture(scope='module')
def kdbpar(kdb):
    path = example_data(os.path.join('start', 'db'))
    assert os.path.exists(path)
    kdb.eval(r'\l %s' % path)
    yield kdb
    kdb.stop()


@pytest.fixture
def df(kdb):
    return kdb.eval('t')


@pytest.fixture
def rdf(kdb):
    return kdb.eval('rt')


@pytest.fixture
def sdf(kdb):
    return kdb.eval('st')
