import pytest

pytest.importorskip('dask.bag')

from operator import methodcaller

from odo import chunks, TextFile, odo
from dask.bag import Bag
from odo.utils import filetexts


def inc(x):
    return x + 1


dsk = {('x', 0): (range, 5),
       ('x', 1): (range, 5),
       ('x', 2): (range, 5)}

L = list(range(5)) * 3

b = Bag(dsk, 'x', 3)


def test_convert_bag_to_list():
    assert odo(b, list) == L


def test_convert_logfiles_to_bag():
    with filetexts({'a1.log': 'Hello\nWorld', 'a2.log': 'Hola\nMundo'}) as fns:
        logs = chunks(TextFile)(list(map(TextFile, fns)))
        b = odo(logs, Bag)
        assert isinstance(b, Bag)
        assert (list(map(methodcaller('strip'), odo(b, list))) ==
                list(map(methodcaller('strip'), odo(logs, list))))


def test_sequence():
    b = odo([1, 2, 3], Bag)
    assert set(b.map(inc)) == set([2, 3, 4])
