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
    assert [func for a, b, func, cost in d.path(int, str)] == [f, g]

    assert list(d.path(int, str)) == list(d.path(1, ''))


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


def test_convert_failure_takes_greedy_path():
    foo = NetworkDispatcher('foo')

    class A(object):
        pass

    class B(object):
        pass

    class C(object):
        pass

    class D(object):
        pass

    discover.register((A, B, C, D))(lambda _: 'int')

    foo.register(B, A, cost=1.0)(lambda _, **__: 1)

    @foo.register(D, A, cost=10.0)
    def expensive_edge(*args, **kwargs):
        raise AssertionError(
            'convert should not take this route because it is more expensive'
            ' than the initial route and greedy route',
        )

    @foo.register(C, B, cost=1.0)
    def badedge(*args, **kwargs):
        raise NotImplementedError()

    @foo.register(D, C, cost=1.0)
    def impossible_edge(*args, **kwargs):
        raise AssertionError(
            'to get to this edge B->C would need to pass which is impossible'
        )

    greedy_retry_route_selected = [False]

    # this edge is more expensive than the cost of B->C->D so it shouldn't
    # be picked until B->C has been removed
    @foo.register(D, B, cost=3.0)
    def greedy_retry_route(data, **kwargs):
        greedy_retry_route_selected[0] = True
        assert data == 1
        return 2

    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        assert foo(D, A()) == 2

    assert greedy_retry_route_selected[0], 'we did not call the expected edge'

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

    assert ([(a, b, cost) for a, b, _, cost in path(foo.graph, A, C)] ==
            [(A, B, 1.0), (B, C, 1.0)])

    ooc = set([A, C])
    assert ([(a, b, cost)
             for a, b, _, cost, in path(foo.graph, A, C, ooc_types=ooc)] ==
            [(A, C, 10.0)])
