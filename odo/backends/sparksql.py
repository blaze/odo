from __future__ import division, print_function, absolute_import

import os
import glob
import itertools
import tempfile
import shutil

from functools import partial
from datetime import datetime, date

import toolz
import datashape
from datashape import dshape, Record, DataShape, Option, Tuple
from datashape.predicates import isdimension, isrecord, iscollection
from toolz.curried import get, map
from toolz import pipe, concat, curry

from .. import append, discover, convert
from ..core import ooc_types
from ..directory import Directory
from .json import JSONLines
from .spark import RDD, SparkDataFrame, SchemaRDD, Dummy, SQLContext


SPARK_ONE_THREE = False


try:
    import pyspark
except ImportError:
    SparkDataFrame = SQLContext = Dummy
    ByteType = ShortType = IntegerType = LongType = FloatType = Dummy
    MapType = DoubleType = StringType = BinaryType = BooleanType = Dummy
    TimestampType = DateType = StructType = ArrayType = StructField = Dummy
else:
    try:
        from pyspark.sql.types import (ByteType, ShortType, IntegerType,
                                       LongType, FloatType, DoubleType,
                                       StringType, BinaryType, BooleanType,
                                       TimestampType, DateType, ArrayType,
                                       StructType, StructField, MapType)
        SPARK_ONE_THREE = True
    except ImportError:
        from pyspark.sql import (ByteType, ShortType, IntegerType, LongType,
                                 FloatType, DoubleType, StringType,
                                 BinaryType, BooleanType, TimestampType,
                                 DateType, ArrayType, StructType,
                                 StructField, MapType)


base = (int, float, datetime, date, bool, str)
_names = ('tmp%d' % i for i in itertools.count())


@append.register(SQLContext, object)
def iterable_to_sql_context(ctx, seq, **kwargs):
    return append(ctx, append(ctx._sc, seq, **kwargs), **kwargs)


def register_table(ctx, srdd, name=None):
    if name is None:
        name = next(_names)
    try:
        # spark 1.3 API
        ctx.registerDataFrameAsTable(srdd, name)
    except AttributeError:
        # spark 1.2 API
        ctx.registerRDDAsTable(srdd, name)


@append.register(SQLContext, (JSONLines, Directory(JSONLines)))
def jsonlines_to_sparksql(ctx, json, dshape=None, name=None, schema=None,
                          samplingRatio=0.25, **kwargs):
    # if we're passing in schema, assume that we know what we're doing and
    # bypass any automated dshape inference
    if dshape is not None and schema is None:
        schema = dshape_to_schema(dshape.measure
                                  if isrecord(dshape.measure) else dshape)
    srdd = ctx.jsonFile(json.path, schema=schema, samplingRatio=samplingRatio)
    register_table(ctx, srdd, name=name)
    return srdd


@convert.register(list, (SparkDataFrame, SchemaRDD), cost=200.0)
def sparksql_dataframe_to_list(df, dshape=None, **kwargs):
    result = df.collect()
    if (dshape is not None and iscollection(dshape) and
            not isrecord(dshape.measure)):
        return list(map(get(0), result))
    return result


@convert.register(base, (SparkDataFrame, SchemaRDD), cost=200.0)
def spark_df_to_base(df, **kwargs):
    return df.collect()[0][0]


@append.register(SQLContext, RDD)
def rdd_to_sqlcontext(ctx, rdd, name=None, dshape=None, **kwargs):
    """ Convert a normal PySpark RDD to a SparkSQL RDD or Spark DataFrame

    Schema inferred by ds_to_sparksql.  Can also specify it explicitly with
    schema keyword argument.
    """
    # TODO: assumes that we don't have e.g., 10 * 10 * {x: int, y: int}
    if isdimension(dshape.parameters[0]):
        dshape = dshape.measure
    sql_schema = dshape_to_schema(dshape)
    sdf = ctx.applySchema(rdd, sql_schema)
    if name is None:
        name = next(_names)
    register_table(ctx, sdf, name=name)

    if SPARK_ONE_THREE:
        # this breaks conversion in Spark 1.2
        ctx.cacheTable(name)
    return sdf


def scala_set_to_set(ctx, x):
    from py4j.java_gateway import java_import

    # import scala
    java_import(ctx._jvm, 'scala')

    # grab Scala's set converter and convert to a Python set
    return set(ctx._jvm.scala.collection.JavaConversions.setAsJavaSet(x))


@discover.register(SQLContext)
def discover_sqlcontext(ctx):
    try:
        table_names = list(map(str, ctx.tableNames()))
    except AttributeError:
        java_names = ctx._ssql_ctx.catalog().tables().keySet()
        table_names = list(scala_set_to_set(ctx, java_names))

    table_names.sort()

    dshapes = zip(table_names, map(discover, map(ctx.table, table_names)))
    return datashape.DataShape(datashape.Record(dshapes))


@discover.register((SparkDataFrame, SchemaRDD))
def discover_spark_data_frame(df):
    schema = df.schema() if callable(df.schema) else df.schema
    return datashape.var * schema_to_dshape(schema)


def chunk_file(filename, chunksize):
    """Stream `filename` in chunks of size `chunksize`.

    Parameters
    ----------
    filename : str
        File to chunk
    chunksize : int
        Number of bytes to hold in memory at a single time
    """
    with open(filename, mode='rb') as f:
        for chunk in iter(partial(f.read, chunksize), ''):
            yield chunk


@append.register(JSONLines, (SparkDataFrame, SchemaRDD))
def spark_df_to_jsonlines(js, df,
                          pattern='part-*', chunksize=1 << 23,  # 8MB
                          **kwargs):
    tmpd = tempfile.mkdtemp()
    try:
        try:
            df.save(tmpd, source='org.apache.spark.sql.json', mode='overwrite')
        except AttributeError:
            shutil.rmtree(tmpd)
            df.toJSON().saveAsTextFile(tmpd)
    except:
        raise
    else:
        files = glob.glob(os.path.join(tmpd, pattern))
        with open(js.path, mode='ab') as f:
            pipe(files,
                 map(curry(chunk_file, chunksize=chunksize)),
                 concat,
                 map(f.write),
                 toolz.count)
    finally:
        shutil.rmtree(tmpd)
    return js


def dshape_to_schema(ds):
    """Convert datashape to SparkSQL type system.

    Examples
    --------
    >>> print(dshape_to_schema('int32'))  # doctest: +SKIP
    IntegerType
    >>> print(dshape_to_schema('5 * int32')  # doctest: +SKIP
    ArrayType(IntegerType,false)
    >>> print(dshape_to_schema('5 * ?int32'))  # doctest: +SKIP
    ArrayType(IntegerType,true)
    >>> print(dshape_to_schema('{name: string, amount: int32}'))  # doctest: +SKIP
    StructType(List(StructField(name,StringType,false),StructField(amount,IntegerType,false)  # doctest: +SKIP))
    >>> print(dshape_to_schema('10 * {name: string, amount: ?int32}'))  # doctest: +SKIP
    ArrayType(StructType(List(StructField(name,StringType,false),StructField(amount,IntegerType,true))),false)
    """
    if isinstance(ds, str):
        return dshape_to_schema(dshape(ds))
    if isinstance(ds, Tuple):
        raise TypeError('Please provide a Record dshape for these column '
                        'types: %s' % (ds.dshapes,))
    if isinstance(ds, Record):
        return StructType([
            StructField(name,
                        dshape_to_schema(deoption(typ)),
                        isinstance(typ, datashape.Option))
            for name, typ in ds.fields])
    if isinstance(ds, DataShape):
        if isdimension(ds[0]):
            elem = ds.subshape[0]
            if isinstance(elem, DataShape) and len(elem) == 1:
                elem = elem[0]
            return ArrayType(dshape_to_schema(deoption(elem)),
                             isinstance(elem, Option))
        else:
            return dshape_to_schema(ds[0])
    if ds in dshape_to_sparksql:
        return dshape_to_sparksql[ds]
    raise NotImplementedError()


def schema_to_dshape(schema):
    if type(schema) in sparksql_to_dshape:
        return sparksql_to_dshape[type(schema)]
    if isinstance(schema, ArrayType):
        dshape = schema_to_dshape(schema.elementType)
        return datashape.var * (Option(dshape)
                                if schema.containsNull else dshape)
    if isinstance(schema, StructType):
        fields = [(field.name, Option(schema_to_dshape(field.dataType))
                  if field.nullable else schema_to_dshape(field.dataType))
                  for field in schema.fields]
        return datashape.dshape(Record(fields))
    raise NotImplementedError('SparkSQL type not known %r' %
                              type(schema).__name__)


def deoption(ds):
    """

    >>> deoption('int32')
    ctype("int32")

    >>> deoption('?int32')
    ctype("int32")
    """
    if isinstance(ds, str):
        ds = dshape(ds)
    if isinstance(ds, DataShape) and not isdimension(ds[0]):
        return deoption(ds[0])
    if isinstance(ds, Option):
        return ds.ty
    else:
        return ds


# see http://spark.apache.org/docs/latest/sql-programming-guide.html#spark-sql-datatype-reference
sparksql_to_dshape = {
    ByteType: datashape.int8,
    ShortType: datashape.int16,
    IntegerType: datashape.int32,
    LongType: datashape.int64,
    FloatType: datashape.float32,
    DoubleType: datashape.float64,
    StringType: datashape.string,
    BinaryType: datashape.bytes_,
    BooleanType: datashape.bool_,
    TimestampType: datashape.datetime_,
    DateType: datashape.date_,
    # sql.ArrayType: ?,
    # sql.MapTYpe: ?,
    # sql.StructType: ?
}

dshape_to_sparksql = {
    datashape.int16: ShortType(),
    datashape.int32: IntegerType(),
    datashape.int64: LongType(),
    datashape.float32: FloatType(),
    datashape.float64: DoubleType(),
    datashape.real: DoubleType(),
    datashape.time_: TimestampType(),
    datashape.date_: DateType(),
    datashape.datetime_: TimestampType(),
    datashape.bool_: BooleanType(),
    datashape.string: StringType()
}

ooc_types |= set([SparkDataFrame, SchemaRDD])
