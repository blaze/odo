from __future__ import absolute_import, division, print_function

import pytest

from odo.into import into
from odo.utils import tmpfile, filetext
from odo.backends.csv import CSV


def test_into_convert():
    assert into(list, (1, 2, 3)) == [1, 2, 3]


def test_into_append():
    lst = []
    result = into(lst, (1, 2, 3))
    assert result == [1, 2, 3]
    assert result is lst


def test_into_curry():
    assert callable(into(list))
    data = (1, 2, 3)
    assert into(list)(data) == into(list, data)


@pytest.mark.parametrize('f', [lambda x: u'%s' % x, lambda x: x])
def test_into_double_string(f):
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert into(list, source) == [('alice', 1), ('bob', 2)]

        with tmpfile('.csv') as target:
            csv = into(f(target), source)
            assert isinstance(csv, CSV)
            with open(target, 'rU') as f:
                assert 'alice' in f.read()


@pytest.mark.parametrize('f', [lambda x: u'%s' % x, lambda x: x])
def test_into_string_on_right(f):
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert into([], f(source)) == [('alice', 1), ('bob', 2)]
