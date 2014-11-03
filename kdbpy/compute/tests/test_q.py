import pytest
from kdbpy import q


@pytest.fixture
def x():
    return q.List(q.Symbol('a'), 1, 2)


@pytest.fixture
def y():
    return q.List(q.Symbol('b'), 2)


def test_qlist_repr(x):
    assert repr(x) == '(`a; 1; 2)'


def test_qlist_sym_repr():
    x = q.List(q.Symbol('a sym'), q.List(q.Symbol('a')), 3)
    assert repr(x) == '(`$"a sym"; (,:[`a]); 3)'


def test_qlist_slice(x):
    assert repr(x[:2]) == '(`a; 1)'


def test_qlist_eq(x):
    assert x == x


def test_qlist_add(x, y):
    z = x + y
    assert z == q.List(q.Symbol('a'), 1, 2, q.Symbol('b'), 2)


def test_qlist_append(x):
    z = x.append(q.Symbol('x'))
    assert z == q.List(q.Symbol('a'), 1, 2, q.Symbol('x'))


def test_qdict():
    x = q.Dict([(q.Symbol('a'), 1), (q.Symbol('b'), 2)])
    assert repr(x) == '(`a; `b)!(1; 2)'


def test_qsymbol():
    s = q.Symbol('a')
    assert repr(s) == '`a'

    s = q.Symbol('a symbol')
    assert repr(s) == '`$"a symbol"'


def test_qstring():
    s = q.String('s')
    assert repr(s) == '"s"'

    s = q.String('"s"')
    assert repr(s) == '"\"s\""'


def test_qbool_equal():
    assert q.Bool() == q.Bool()
    assert q.Bool(False) == q.Bool()
    assert q.Bool(True) == q.Bool(True)
    assert q.Bool(False) != q.Bool(True)
    assert q.Bool([]) == q.Bool({})
    assert q.Bool() == q.Bool(0)
