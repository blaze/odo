from __future__ import division, print_function, absolute_import


class Dummy(object):
    pass

try:
    from pyspark import SparkContext
    import pyspark
    from pyspark import RDD
    from pyspark.rdd import PipelinedRDD
    from pyspark.sql import SchemaRDD, SQLContext
    RDD.min
except (AttributeError, ImportError):
    SchemaRDD = PipelinedRDD = RDD = SparkContext = SQLContext = Dummy
    pyspark = Dummy()


from toolz import memoize
from datashape import var

from .. import convert, append, discover


@append.register(SparkContext, list)
def list_to_spark_context(sc, seq, **kwargs):
    return sc.parallelize(seq)


@append.register(SparkContext, object)
def anything_to_spark_context(sc, o, **kwargs):
    return append(sc, convert(list, o, **kwargs), **kwargs)


@convert.register(list, (RDD, PipelinedRDD, SchemaRDD))
def rdd_to_list(rdd, **kwargs):
    return rdd.collect()


@discover.register(RDD)
def discover_rdd(rdd, n=50, **kwargs):
    data = rdd.take(n)
    return var * discover(data).subshape[0]


SQLContext = memoize(SQLContext)


@convert.register(SchemaRDD, (RDD, PipelinedRDD))
def rdd_to_schema_rdd(rdd, **kwargs):
    return append(SQLContext(rdd.context), rdd, **kwargs)
