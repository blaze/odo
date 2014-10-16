import getpass
import pytest

import numpy as np
from blaze import Symbol, compute
from kdbpy import into, resource, Q


@pytest.fixture(scope='module')
def raw():
    return np.array([(1, -100.90, 'Bob'),
                     (2, 300.23, 'Alice'),
                     (3, 432.2, 'Joe')],
                    dtype=[('id', 'int64'),
                           ('amount', 'float64'),
                           ('name', 'object')])


@pytest.fixture(scope='module')
def t():
    return Symbol('t', 'var * {id: int, amount: float64, name: string}')


@pytest.fixture(scope='module')
def q(t):
    return Q(t=t._name, q=[])


def test_projection(t, q):
    expr = t[['a', 'b']]
    result = compute(expr, q)
    assert result == 'select a, b from t'


def test_selection(t, q):
    expr = t[t.a > 1.0]
    result = compute(expr, q)
    assert result == 'select from t where t.a > 1.0'


def test_complex_selection_projection(t, q):
    expr = t[(t.id + 1 - 2* t.id * t.id + t.amount) > t.id - 3][['id',
                                                                 'amount']]
    result = compute(expr, q)
    assert result == 'select id, amount from t where t.id + 1 - 2 * t.id * t.id + t.amount > 1.id - 3'
