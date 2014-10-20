import pytest

import numpy as np
from blaze import Symbol, compute, by
from kdbpy import Q


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
    return Q(t=t, q=[])


def test_projection(t, q):
    expr = t[['id', 'amount']]
    result = compute(expr, q)
    assert result == '(?; `t; 0b; (`id; `amount)!(`id; `amount))'


def test_single_projection(t, q):
    expr = t[['id']]
    result = compute(expr, q)
    assert result == '(?; `t; 0b; (enlist[`id])!(enlist[`id]))'


def test_selection(t, q):
    expr = t[t.id == 1]
    result = compute(expr, q)
    assert result == '(?; `t; (enlist[(enlist[(=; `t.id; 1)])]); 0b; ())'


def test_broadcast(t, q):
    expr = t.id + 1
    result = compute(expr, q)
    assert result == '(+; `t.id; 1)'


def test_complex_selection_projection(t, q):
    expr = t[t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3][['id', 'amount']]
    result = compute(expr, q)
    import ipdb; ipdb.set_trace()
    e = 'enlist[(enlist[(>; (+; `t.id; (-; 1; (*; 2; (+; (xexp; `t.id; 2); `t.amount)))); (-; `t.id; 3))])]'
    expected = '(?; `t; %s; 0b; (`id; `amount)!(`id; `amount))' % e
    assert result == expected


def test_unary_op(t, q):
    expr = -t.amount
    result = compute(expr, q)
    assert result == '(-:; `t.amount)'


def test_string(t, q):
    expr = t.name == 'Alice'
    result = compute(expr, q)
    expected = '(=; `t.name; `Alice)'
    assert result == expected


@pytest.mark.xfail
def test_simple_by(t, q):
    expr = by(t.name, t.amount.sum())
    result = compute(expr, q)
    assert result == False
    assert result == 'select (sum (t.amount)) by (t.name) from (t)'


def test_field(t, q):
    expr = t.name
    result = compute(expr, q)
    assert result == '`t.name'


def test_sum(t, q):
    expr = t.amount.sum()
    result = compute(expr, q)
    assert result == "(sum; `t.amount)"
