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


from toolz import memoize, drop
from datashape import var

from .csv import CSV, infer_header, get_delimiter
from .aws import S3, get_s3n_path
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


@append.register(SparkContext, S3(CSV))
def s3_csv_to_rdd(sc, s3, dshape=None, use_unicode=False, minPartitions=None,
                  **kwargs):
    """Convert an S3(CSV) file to a Spark RDD.

    Notes
    -----
    Performs no type inference.
    """
    # spark only accepts s3n://
    path = get_s3n_path(s3.path)

    # load up our CSV file
    rdd = sc.textFile(path, minPartitions=minPartitions,
                      use_unicode=use_unicode)

    sep = s3.dialect.get('delimiter') or get_delimiter(s3)

    # split by our delimiter and strip off extra whitespace
    split = rdd.map(lambda x, sep=sep: tuple(s.strip() for s in x.split(sep)))

    # remove the first line if we have a header, assuming it's on the first
    # partition
    if infer_header(s3) if s3.has_header is None else s3.has_header:
        return split.mapPartitionsWithIndex(lambda idx, it: (drop(1, it)
                                                             if not idx
                                                             else it))
    return split
