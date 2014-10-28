import os
import shutil
import getpass
import socket

import pytest
import blaze as bz
from kdbpy.kdb import KQ, get_credentials
from kdbpy.compute.qtable import QTable


@pytest.fixture
def t():
    return bz.Symbol('t', 'var * {name: string, id: int64, amount: float64}')


@pytest.fixture
def rt():
    return bz.Symbol('rt', 'var * {name: string, tax: float64, street: string}')


@pytest.fixture
def st():
    return bz.Symbol('st', 'var * {name: string, jobcode: int64, tree: string}')


@pytest.yield_fixture(scope='module')
def kdb():
    r = KQ(start=True)
    r.eval('t: ([] '
           'name: 10 ? `Bob`Alice`Joe`Smithers;'
           'id: 1 + til 10;'
           'amount: 10 ? 10.0)')
    r.eval('rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2; '
           'street: `maple`apple`pine`grove)')
    r.eval('st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; '
           'tree: `maple`apple`pine)')
    yield r
    r.stop()


@pytest.fixture
def q(kdb):
    return QTable('kdb://pcloud@localhost:5001', name='t', engine=kdb)


@pytest.fixture
def rq(kdb):
    return QTable('kdb://pcloud@localhost:5001', name='rt', engine=kdb)


@pytest.fixture
def sq(kdb):
    return QTable('kdb://pcloud@localhost:5001', name='st', engine=kdb)


@pytest.fixture
def df(kdb):
    return kdb.eval('t')


@pytest.fixture
def rdf(kdb):
    return kdb.eval('rt')


@pytest.fixture
def sdf(kdb):
    return kdb.eval('st')


@pytest.fixture
def rstring():
    return 'kdb://%s@%s:5000' % (getpass.getuser(), socket.gethostname())


@pytest.fixture
def tstring(rstring):
    return rstring + '::t'


@pytest.yield_fixture(scope='module')
def kdbpar():
    kq = KQ(get_credentials(), start='restart')
    kq.eval(r'\l buildhdb.q')
    kq.eval(r'\l %s' % os.path.join('start', 'db'))
    yield kq
    kq.stop()
    shutil.rmtree(os.path.abspath('start'))
