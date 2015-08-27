from __future__ import print_function, absolute_import, division

import pytest

pytest.importorskip('pyspark')

import pytest
from datashape import dshape
from odo import odo, discover
from odo.backends.sparksql import SparkDataFrame
from pyspark import RDD
from pyspark.rdd import PipelinedRDD
from pyspark.sql import Row, SchemaRDD


data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3]]


@pytest.fixture
def rdd(sc):
    return sc.parallelize(data)


def test_spark_into(rdd):
    with pytest.raises(NotImplementedError):
        odo([1, 2, 3], rdd)


def test_spark_into_context(sc):
    seq = [1, 2, 3]
    rdd = odo(seq, sc)
    assert isinstance(rdd, RDD)
    assert odo(rdd, []) == seq


def test_rdd_into_schema_rdd(rdd):
    ds = dshape('var * {name: string, amount: float64, id: int64}')
    srdd = odo(rdd, SparkDataFrame, dshape=ds)
    assert isinstance(srdd, (SparkDataFrame, SchemaRDD))
    assert list(map(set, srdd.collect())) == list(map(set, rdd.collect()))


def test_pipelined_rdd_into_schema_rdd(rdd):
    pipelined = rdd.map(lambda x: Row(amount=x[1]))
    assert isinstance(pipelined, PipelinedRDD)

    srdd = odo(pipelined, SparkDataFrame,
               dshape=dshape('var * {amount: float64}'))

    assert isinstance(srdd, (SparkDataFrame, SchemaRDD))
    assert (list(map(set, srdd.collect())) ==
            list(map(set, pipelined.collect())))


def test_discover_rdd(rdd):
    assert discover(rdd).subshape[0] == discover(data).subshape[0]
