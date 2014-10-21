from __future__ import print_function, division, absolute_import
import pytest

import numpy as np
from blaze import Symbol, compute, by
from kdbpy import Q, QString, QList, QDict, QSymbol, QBool


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


def test_qlist():
    x = QList(QSymbol('a'), 1, 2)
    assert repr(x) == '(`a; 1; 2)'

    x = QList(QSymbol('a sym'), QList(QSymbol('a')), 3)
    assert repr(x) == '(`$"a sym"; (enlist[`a]); 3)'


def test_qdict():
    x = QDict([(QSymbol('a'), 1), (QSymbol('b'), 2)])
    assert repr(x) == '(`a; `b)!(1; 2)'


def test_qsymbol():
    s = QSymbol('a')
    assert repr(s) == '`a'

    s = QSymbol('a symbol')
    assert repr(s) == '`$"a symbol"'


def test_qstring():
    s = QString('s')
    assert repr(s) == '"s"'

    s = QString('"s"')
    assert repr(s) == '"\"s\""'


def test_projection(t, q):
    expr = t[['id', 'amount']]
    result = compute(expr, q)
    fields = [QSymbol('id'), QSymbol('amount')]
    expected = QList('?', QSymbol('t'), QList(), QBool(False),
                     QDict(list(zip(fields, fields))))
    import ipdb; ipdb.set_trace()
    assert result == expected


def test_single_projection(t, q):
    expr = t[['id']]
    result = compute(expr, q)
    assert result == QList('?', QSymbol('t'), QList(),
                           QBool(False), QDict([(QSymbol('id'),),
                                                (QSymbol('id'),)]))


def test_selection(t, q):
    expr = t[t.id == 1]
    result = compute(expr, q)
    assert result == '(?; `t; (enlist[(enlist[(=; `t.id; 1)])]); 0b; ())'


def test_broadcast(t, q):
    expr = t.id + 1
    result = compute(expr, q)
    assert result == '(+; `t.id; 1)'


def test_complex_arith(t, q):
    expr = t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3
    result = compute(expr, q)
    big = '(+; (-; (+; `t.id; 1); (*; 2; (xexp; `t.id; 2))); `t.amount)'
    expected = '(>; %s; (-; `t.id; 3))' % big
    assert result == expected


def test_complex_selection(t, q):
    expr = t[t.id + 1 - 2 * t.id ** 2 + t.amount > t.id - 3]
    result = compute(expr, q)
    big = '(+; (-; (+; `t.id; 1); (*; 2; (xexp; `t.id; 2))); `t.amount)'
    where = '(>; %s; (-; `t.id; 3))' % big
    assert result == '(?; `t; (enlist[(enlist[%s])]); 0b; ())' % where


def test_complex_selection_projection(t, q):
    expr = t[t.id ** 2 + t.amount > t.id - 3][['id', 'amount']]
    result = compute(expr, q)
    child = QList()
    constraint = QList()
    by = QBool(False)
    fields = QSymbol('id'), QSymbol('amount')
    expected = QList('?', child, constraint, by,
                     QDict(list(zip(fields, fields))))
    assert result == expected



def test_unary_op(t, q):
    expr = -t.amount
    result = compute(expr, q)
    assert result == '(-:; `t.amount)'


def test_string_compare(t, q):
    expr = t.name == 'Alice'
    result = compute(expr, q)
    expected = '(=; `t.name; "Alice")'
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
