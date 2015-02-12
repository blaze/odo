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
    with pytest.raises(NotImplementedError):
        into(rdd, [1, 2, 3])


def test_spark_into_context(sc):
    seq = [1, 2, 3]
    rdd = into(sc, seq)
    assert isinstance(rdd, RDD)
    assert into([], rdd) == seq


def test_discover_rdd(rdd):
    assert discover(rdd).subshape[0] == discover(data).subshape[0]
