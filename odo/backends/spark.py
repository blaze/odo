from __future__ import division, print_function, absolute_import

from datashape import var

from .. import convert, append, discover

from pyspark import SparkContext
from pyspark import RDD
from pyspark.rdd import PipelinedRDD


@append.register(SparkContext, list)
def list_to_spark_context(sc, seq, **kwargs):
    return sc.parallelize(seq)


@append.register(SparkContext, object)
def anything_to_spark_context(sc, o, **kwargs):
    return append(sc, convert(list, o, **kwargs), **kwargs)


@convert.register(list, (RDD, PipelinedRDD))
def rdd_to_list(rdd, **kwargs):
    return rdd.collect()


@discover.register(RDD)
def discover_rdd(rdd, n=50, **kwargs):
    data = rdd.take(n)
    return var * discover(data).subshape[0]
