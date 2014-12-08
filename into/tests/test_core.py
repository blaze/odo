from into.core import NetworkDispatcher

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
