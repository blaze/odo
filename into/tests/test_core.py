from into.core import NetworkDispatcher
from datashape import discover

def test_basic():
    d = NetworkDispatcher('foo')

    @d.register(float, int, cost=1.0)
    def f(x, **kwargs):
        return float(x)

    @d.register(str, float, cost=1.0)
    def g(x, **kwargs):
        return str(x)

    assert [func for a, b, func in d.path(int, str)] == [f, g]

    assert d.func(int, str)(1) == '1.0'


def test_convert_is_robust_to_failures():
    foo = NetworkDispatcher('foo')

    class A(object): pass
    class B(object): pass
    class C(object): pass
    discover.register((A, B, C))(lambda x: 'int')
    foo.register(B, A, cost=1.0)(lambda x, **kwargs: 1)
    foo.register(C, B, cost=1.0)(lambda x, **kwargs: x / 0) # note that this errs
    foo.register(C, A, cost=10.0)(lambda x, **kwargs: 2)

    assert foo(C, A()) == 2
