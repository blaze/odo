from __future__ import absolute_import, division, print_function

import pytest

import numpy as np

from odo import odo, into
from odo.utils import tmpfile, filetext
from odo.backends.csv import CSV


def test_into_convert():
    assert odo((1, 2, 3), list) == [1, 2, 3]


def test_into_append():
    lst = []
    result = odo((1, 2, 3), lst)
    assert result == [1, 2, 3]
    assert result is lst


def test_into_append_failure():
    nd = np.array([1, 2, 3])
    with pytest.raises(TypeError):
        odo((4, 5), nd)


def test_into_curry():
    assert callable(into(list))
    data = (1, 2, 3)
    assert into(list)(data) == odo(data, list)


@pytest.mark.parametrize('f', [lambda x: u'%s' % x, lambda x: x])
def test_into_double_string(f):
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert odo(source, list) == [('alice', 1), ('bob', 2)]

        with tmpfile('.csv') as target:
            csv = odo(source, f(target))
            assert isinstance(csv, CSV)
            with open(target, 'rU') as f:
                assert 'alice' in f.read()


@pytest.mark.parametrize('f', [lambda x: u'%s' % x, lambda x: x])
def test_into_string_on_right(f):
    with filetext('alice,1\nbob,2', extension='.csv') as source:
        assert odo(f(source), []) == [('alice', 1), ('bob', 2)]


def test_into_string_dshape():
    np.testing.assert_array_equal(odo([1, 2, 3], np.ndarray,
                                      dshape='var * float64'),
                                  np.array([1, 2, 3], dtype='float64'))


@pytest.mark.parametrize('dshape', [1, object()])
def test_into_invalid_dshape(dshape):
    with pytest.raises(TypeError):
        into(list, (1, 2, 3), dshape=dshape)
