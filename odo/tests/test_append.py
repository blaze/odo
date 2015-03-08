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
