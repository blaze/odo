from __future__ import absolute_import, division, print_function
import warnings

from odo.core import NetworkDispatcher, path, FailedConversionWarning
from datashape import discover

d = NetworkDispatcher('foo')

@d.register(float, int, cost=1.0)
def f(x, **kwargs):
    return float(x)

@d.register(str, float, cost=1.0)
def g(x, **kwargs):
    return str(x)


def test_basic():
    assert [func for a, b, func in d.path(int, str)] == [f, g]

    assert d.path(int, str) == d.path(1, '')


def test_convert_is_robust_to_failures():
    foo = NetworkDispatcher('foo')

    def badfunc(*args, **kwargs):
        raise NotImplementedError()

    class A(object): pass
    class B(object): pass
    class C(object): pass
    discover.register((A, B, C))(lambda x: 'int')
    foo.register(B, A, cost=1.0)(lambda x, **kwargs: 1)
    foo.register(C, B, cost=1.0)(badfunc)
    foo.register(C, A, cost=10.0)(lambda x, **kwargs: 2)

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        assert foo(C, A()) == 2

    assert len(ws) == 1
    w = ws[0].message
    assert isinstance(w, FailedConversionWarning)
    assert 'B -> C' in str(w)


def test_ooc_behavior():
    foo = NetworkDispatcher('foo')
    class A(object): pass
    class B(object): pass
    class C(object): pass

    discover.register((A, B, C))(lambda x: 'int')
    foo.register(B, A, cost=1.0)(lambda x, **kwargs: 1)
    foo.register(C, B, cost=1.0)(lambda x, **kwargs: x / 0) # note that this errs
    foo.register(C, A, cost=10.0)(lambda x, **kwargs: 2)

    assert [(a, b) for a, b, func in path(foo.graph, A, C)] == [(A, B), (B, C)]

    ooc = set([A, C])
    assert [(a, b) for a, b, func in path(foo.graph, A, C, ooc_types=ooc)] == \
                        [(A, C)]
