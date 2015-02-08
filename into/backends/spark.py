class Dummy(object):
    sum = max = min = count = distinct = mean = variance = stdev = None

try:
    from pyspark import SparkContext
    import pyspark
    from pyspark.rdd import RDD
    RDD.min
except (AttributeError, ImportError):
    SparkContext = Dummy
    pyspark = Dummy()
    pyspark.rdd = Dummy()
    RDD = Dummy


from collections import Iterator

from datashape import var

from ..into import convert, append
from ..core import discover


@append.register(SparkContext, (list, tuple, Iterator))
def iterable_to_spark_context(sc, seq):
    return sc.parallelize(seq)


@append.register(RDD, (list, tuple))
def sequence_to_rdd(rdd, seq):
    # Hm this seems anti-pattern-y
    return append(rdd.context, seq)


@convert.register(list, RDD)
def rdd_to_list(rdd):
    return rdd.collect()


@discover.register(RDD)
def discover_rdd(rdd):
    data = rdd.take(50)
    return var * discover(data).subshape[0]


