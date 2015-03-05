from __future__ import absolute_import, division, print_function

from odo.into import into
from odo.utils import tmpfile, filetext
from odo.backends.csv import CSV

def test_into_convert():
    assert into(list, (1, 2, 3)) == [1, 2, 3]


def test_into_append():
    L = []
    result = into(L, (1, 2, 3))
    assert result == [1, 2, 3]
    assert result is L


def test_into_curry():
    assert callable(into(list))
    data = (1, 2, 3)
    assert into(list)(data) == into(list, data)


def test_into_double_string():
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert into(list, source) == [('alice', 1), ('bob', 2)]

        with tmpfile('.csv') as target:
            csv = into(target, source)
            assert isinstance(csv, CSV)
            with open(target) as f:
                assert 'alice' in f.read()


def test_into_string_on_right():
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert into([], source) == [('alice', 1), ('bob', 2)]
