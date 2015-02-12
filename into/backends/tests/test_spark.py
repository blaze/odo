from __future__ import print_function, absolute_import, division

import pytest
from into import into, discover
from into.backends.spark import RDD


data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3]]


@pytest.fixture
def rdd(sc):
    return sc.parallelize(data)


def test_spark_into(rdd):
    seq = [1, 2, 3]
    assert isinstance(into(rdd, seq), RDD)
    assert into([], into(rdd, seq)) == seq


def test_discover_rdd(rdd):
    assert discover(rdd).subshape[0] == discover(data).subshape[0]
