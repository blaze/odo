from __future__ import division, print_function, absolute_import

from toolz import memoize
from datashape import var

from .. import convert, append, discover


class Dummy(object):
    pass

try:
    from pyspark import SparkContext
    import pyspark
    from pyspark import RDD
    from pyspark.rdd import PipelinedRDD
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        SparkDataFrame = Dummy
    from pyspark.sql import SchemaRDD
    from pyspark.sql import SQLContext, HiveContext
    RDD.min
except (AttributeError, ImportError):
    SparkDataFrame = PipelinedRDD = RDD = SparkContext = SQLContext = Dummy
    HiveContext = SchemaRDD = Dummy
    pyspark = Dummy()
else:
    HiveContext = memoize(HiveContext)


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


@convert.register((SparkDataFrame, SchemaRDD), (RDD, PipelinedRDD))
def rdd_to_spark_df_or_srdd(rdd, **kwargs):
    return append(HiveContext(rdd.context), rdd, **kwargs)
