from into import into
from pyspark import RDD


def test_spark_into(rdd):
    seq = [1, 2, 3]
    assert isinstance(into(rdd, seq), RDD)
    assert into([], into(rdd, seq)) == seq
