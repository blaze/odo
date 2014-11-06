import getpass
import socket

import pytest


@pytest.fixture
def t():
    bz = pytest.importorskip('blaze')
    return bz.Symbol('t', 'var * {name: string, id: int64, amount: float64, '
                     'when: datetime, on: date}')


@pytest.fixture
def rt():
    bz = pytest.importorskip('blaze')
    return bz.Symbol('rt', 'var * {name: string, tax: float64, street: string}')


@pytest.fixture
def st():
    bz = pytest.importorskip('blaze')
    return bz.Symbol('st', 'var * {name: string, jobcode: int64, tree: string, '
                     'alias: string}')


@pytest.fixture
def q(kdb):
    pytest.importorskip('kdbpy.compute')
    from kdbpy.compute.qtable import QTable
    return QTable('kdb://pcloud@localhost:5001', name='t', engine=kdb)


@pytest.fixture
def rq(kdb):
    pytest.importorskip('kdbpy.compute')
    from kdbpy.compute.qtable import QTable
    return QTable('kdb://pcloud@localhost:5001', name='rt', engine=kdb)


@pytest.fixture
def sq(kdb):
    pytest.importorskip('kdbpy.compute')
    from kdbpy.compute.qtable import QTable
    return QTable('kdb://pcloud@localhost:5001', name='st', engine=kdb)


@pytest.fixture
def rstring():
    return 'kdb://%s@%s:5000' % (getpass.getuser(), socket.gethostname())
