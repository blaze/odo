from into.chunks import *
from toolz import first


IL = iterable(list)


def test_chunks_basics():
    assert isinstance(IL, type)
    assert issubclass(IL, IterableOf)


def test_chunks_isnt_consumable():
    il = IL([[1, 2, 3], [4, 5, 6]])

    assert next(iter(il)) == [1, 2, 3]
    assert next(iter(il)) == [1, 2, 3]


def test_chunks_is_memoized():
    assert iterable(list) is iterable(list)


def test_callables():
    il = IL(lambda: (list(range(3)) for i in range(3)))

    assert first(il) == [0, 1, 2]
    assert first(il) == [0, 1, 2]


def test_discover():
    il = IL([[1, 2, 3], [4, 5, 6]])
    assert discover(il).measure == discover(1).measure
