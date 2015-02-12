from __future__ import division, print_function, absolute_import

from collections import Iterator
import pandas as pd
import itertools
import datashape
from datashape import dshape, Record, DataShape, Option, Tuple
from datashape.predicates import isdimension
from toolz import valmap

from ..convert import convert
from ..append import append
from ..core import discover
from ..into import convert, append


class Dummy(object):
    pass


try:
    from pyspark.sql import SQLContext, SchemaRDD
    from pyspark.sql import (ByteType, ShortType, IntegerType, LongType,
                             FloatType, DoubleType, StringType, BinaryType,
                             BooleanType, TimestampType, DateType, ArrayType,
                             StructType, StructField, MapType)
    from pyspark import RDD
except ImportError:
    SchemaRDD = SQLContext = Dummy
    ByteType = ShortType = IntegerType = LongType = FloatType = Dummy()
    MapType = DoubleType = StringType = BinaryType = BooleanType = Dummy()
    TimestampType = DateType = StructType = ArrayType = StructField = Dummy()


@convert.register(pd.DataFrame, SchemaRDD)
def sparksql_dataframe_to_pandas_dataframe(rdd, **kwargs):
    return pd.DataFrame(convert(list, rdd, **kwargs), columns=rdd.columns)


_names = ('tmp%d' % i for i in itertools.count())


@append.register(SQLContext, (list, tuple, Iterator))
def iterable_to_sql_context(ctx, seq, **kwargs):
    return append(ctx, ctx._sc.parallelize(seq), **kwargs)


@append.register(SQLContext, RDD)
def rdd_to_sqlcontext(ctx, rdd, name=None, **kwargs):
    """ Convert a normal PySpark RDD to a SparkSQL RDD

    Schema inferred by ds_to_sparksql.  Can also specify it explicitly with
    schema keyword argument.
    """
    sql_schema = dshape_to_schema(kwargs['dshape'])
    sdf = ctx.applySchema(rdd, sql_schema)
    ctx.registerRDDAsTable(sdf, name or next(_names))
    return sdf


@discover.register(SQLContext)
def discover_sqlcontext(ctx):
    dshapes = valmap(discover, get_catalog(ctx))
    return datashape.DataShape(datashape.Record(dshapes.items()))


@discover.register(SchemaRDD)
def discover_spark_data_frame(df):
    return datashape.var * schema_to_dshape(df.schema())


def dshape_to_schema(ds):
    """Convert datashape to SparkSQL type system.

    Examples
    --------
    >>> print(dshape_to_schema('int32'))
    IntegerType
    >>> print(dshape_to_schema('5 * int32'))
    ArrayType(IntegerType,false)
    >>> print(dshape_to_schema('5 * ?int32'))
    ArrayType(IntegerType,true)
    >>> print(dshape_to_schema('{name: string, amount: int32}'))
    StructType(List(StructField(name,StringType,false),StructField(amount,IntegerType,false)))
    >>> print(dshape_to_schema('10 * {name: string, amount: ?int32}'))
    ArrayType(StructType(List(StructField(name,StringType,false),StructField(amount,IntegerType,true))),false)
    """
    if isinstance(ds, str):
        return dshape_to_schema(dshape(ds))
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
    if schema in sparksql_to_dshape:
        return sparksql_to_dshape[schema]
    if isinstance(schema, ArrayType):
        dshape = schema_to_dshape(schema.elementType)
        return datashape.var * (Option(dshape)
                                if schema.containsNull else dshape)
    if isinstance(schema, StructType):
        return dshape(Record([(field.name,
                               Option(schema_to_dshape(field.dataType))
                               if field.nullable
                               else schema_to_dshape(field.dataType))
                              for field in schema.fields]))
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


@discover.register(SchemaRDD)
def discover_spark_data_frame(df):
    return datashape.var * schema_to_dshape(df.schema())


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


def scala_set_to_set(ctx, x):
    from py4j.java_gateway import java_import

    # import scala
    java_import(ctx._jvm, 'scala')

    # grab Scala's set converter and convert to a Python set
    return set(ctx._jvm.scala.collection.JavaConversions.setAsJavaSet(x))


def get_catalog(ctx):
    # the .catalog() method yields a SimpleCatalog instance. This class is
    # hidden from the Python side, but is present in the public Scala API
    java_names = ctx._ssql_ctx.catalog().tables().keySet()
    table_names = scala_set_to_set(ctx, java_names)
    tables = map(ctx.table, table_names)
    return dict(sorted(zip(table_names, tables)))
