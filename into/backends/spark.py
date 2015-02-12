class Dummy(object):
    pass

try:
    from pyspark import SparkContext
    import pyspark
    from pyspark import RDD
    from pyspark.rdd import PipelinedRDD
    from pyspark.sql import SchemaRDD
    RDD.min
except (AttributeError, ImportError):
    SchemaRDD = PipelinedRDD = RDD = SparkContext = Dummy
    pyspark = Dummy()


from collections import Iterator

from datashape import var

from ..into import convert, append
from ..core import discover


@append.register(SparkContext, (list, tuple, Iterator))
def iterable_to_spark_context(sc, seq, **kwargs):
    return sc.parallelize(seq)


@append.register(RDD, (list, tuple))
def sequence_to_rdd(rdd, seq, **kwargs):
    # Hm this seems anti-pattern-y
    return append(rdd.context, seq)


@convert.register(list, (RDD, PipelinedRDD, SchemaRDD))
def rdd_to_list(rdd, **kwargs):
    return rdd.collect()


@discover.register(RDD)
def discover_rdd(rdd, n=50, **kwargs):
    data = rdd.take(n)
    return var * discover(data).subshape[0]
