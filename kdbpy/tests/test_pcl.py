import unittest
import pytest
from kdbpy import pcl
from pandas import DataFrame, Series
from pandas.util.testing import assert_frame_equal, assert_series_equal
import numpy as np

def compare(a,b):
    assert type(a) == type(b)
    if isinstance(a, DataFrame):
        assert_frame_equal(a,b)
    elif isinstance(a, Series):
        assert_series_equal(a,b)
    elif isinstance(a, np.array):
        np.array_equal(a,b)
    else:
        assert a == b

class PCL(unittest.TestCase):

    def test_start_kdb(self):
        p = pcl.PCL(start_kdb=True)
        assert p.is_kdb

        # repr
        result = str(p)
        assert 'KDB: [Credentials(' in result
        assert '-> connected' in result

        p.stop()
        assert not p.is_kdb

        result = str(p)
        assert 'KDB: [client/server not started]'

    def test_examples(self):

        # examples
        with pcl.PCL() as p:

            for k, e in p.examples:

                result = p.eval(e.q)
                compare(result,e.result)
