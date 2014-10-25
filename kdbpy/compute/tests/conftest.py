import pytest
import blaze as bz
from kdbpy.kdb import KQ
from kdbpy.compute.qtable import QTable
import pandas as pd
import numpy as np


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
    r.eval('t: ([] name: `Bob`Alice`Joe; id: 1 2 3; amount: -100.90 0n 432.2)')
    r.eval('rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2; '
           'street: `maple`apple`pine`grove)')
    r.eval('st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; '
           'tree: `maple`apple`pine)')
    yield r
    r.stop()


@pytest.fixture
def q(kdb):
    return QTable('kdb://pcloud@localhost:5001::t', engine=kdb)


@pytest.fixture
def rq(kdb):
    return QTable('kdb://pcloud@localhost:5001::rt', engine=kdb)


@pytest.fixture
def sq(kdb):
    return QTable('kdb://pcloud@localhost:5001::st', engine=kdb)


@pytest.fixture
def df():
    return pd.DataFrame([('Bob', 1, -100.90),
                         ('Alice', 2, np.nan),
                         ('Joe', 3, 432.2)],
                        columns=['name', 'id', 'amount'])


@pytest.fixture
def rdf():
    return pd.DataFrame([('Bob', -3.1, 'maple'),
                         ('Alice', 2.0, 'apple'),
                         ('Joe', np.nan, 'pine'),
                         ('John', 4.2, 'grove')],
                        columns=['name', 'tax', 'street']).set_index('name',
                                                                     drop=True)


@pytest.fixture
def sdf():
    return pd.DataFrame([('Bob', 9, 'maple'),
                         ('Alice', 10, 'apple'),
                         ('Joe', 11, 'pine')],
                        columns=['name', 'id', 'street']).set_index('name',
                                                                    drop=True)


