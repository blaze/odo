import pytest

import blaze as bz
from kdbpy.compute.qtable import QTable


@pytest.fixture
def t():
    return bz.symbol('t', 'var * {name: string, id: int64, amount: float64, '
                     'when: datetime, on: date}')


@pytest.fixture
def rt():
    return bz.symbol('rt', 'var * {name: string, tax: float64, street: string}')


@pytest.fixture
def st():
    return bz.symbol('st', 'var * {name: string, jobcode: int64, tree: string, '
                     'alias: string}')


@pytest.fixture
def q(rstring, kdb):
    return QTable(tablename='t', engine=kdb)


@pytest.fixture
def rq(kdb):
    return QTable(tablename='rt', engine=kdb)


@pytest.fixture
def sq(rstring, kdb):
    return QTable(tablename='st', engine=kdb)


@pytest.fixture
def ktq(rstring, kdb):
    return QTable(tablename='kt', engine=kdb)
