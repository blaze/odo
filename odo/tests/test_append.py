from __future__ import absolute_import, division, print_function

from odo.append import append

def test_append_list():
    L = [1, 2, 3]
    append(L, [4, 5, 6])
    assert L == [1, 2, 3, 4, 5, 6]


def test_append_list_to_set():
    s = set([1, 2, 3])
    append(s, [4, 5, 6])
    assert s == set([1, 2, 3, 4, 5, 6])


def test_append_set_to_list():
    s = set([3, 4, 5])
    lst = [1, 2, 3]
    append(lst, s)
    assert sorted(lst) == [1, 2, 3, 3, 4, 5]


def test_append_tuple_to_set():
    s = set([1, 2, 3])
    append(s, (3, 4, 5))
    assert s == set([1, 2, 3, 4, 5])
