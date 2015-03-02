from __future__ import absolute_import, division, print_function

import os
from into.resource import resource
from into.utils import raises


def test_raises_not_implemented_error():
    assert raises(NotImplementedError,
            lambda: resource('adsfs://5sdjkg9yg35420shfg083.3923.925y2560!'))
