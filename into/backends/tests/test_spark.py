import pytest
from into import into
from pyspark import RDD


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
