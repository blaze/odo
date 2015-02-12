
import pytest
import pandas as pd

pyspark = pytest.importorskip('pyspark')

from pyspark.sql import SchemaRDD, Row
from pyspark.sql import ArrayType, StructField, StructType, IntegerType
from pyspark.sql import StringType

import datashape
from datashape import dshape
from into import into, discover
from into.utils import tmpfile
from into.backends.sparksql import schema_to_dshape, dshape_to_schema


data = [['Alice', 100.0, 1],
        ['Bob', 200.0, 2],
        ['Alice', 50.0, 3]]


df = pd.DataFrame(data, columns=['name', 'amount', 'id'])


@pytest.yield_fixture(scope='module')
def people(sc):
    with tmpfile('.txt') as fn:
        df.to_csv(fn, header=False, index=False)
        raw = sc.textFile(fn)
        parts = raw.map(lambda line: line.split(','))
        yield parts.map(lambda person: Row(name=person[0],
                                           amount=float(person[1]),
                                           id=int(person[2])))


@pytest.fixture(scope='module')
def ctx(sqlctx, people):
    schema = sqlctx.inferSchema(people)
    schema.registerTempTable('t')
    return sqlctx


def test_pyspark_to_sparksql(ctx, people):
    sdf = into(ctx, data)
    assert isinstance(sdf, SchemaRDD)
    assert into(list, people) == into(list, sdf)


def test_into_sparksql_from_other(ctx):
    sdf = into(ctx, df)
    assert isinstance(sdf, SchemaRDD)
    assert into(list, sdf) == into(list, df)


def test_discover_context(ctx):
    result = discover(ctx)
    assert result is not None


def test_schema_to_dshape():
    assert schema_to_dshape(IntegerType()) == int64

    assert schema_to_dshape(
        ArrayType(IntegerType(), False)) == dshape("var * int64")

    assert schema_to_dshape(
        ArrayType(IntegerType(), True)) == dshape("var * ?int64")

    assert schema_to_dshape(StructType([
        StructField('name', StringType(), False),
        StructField('amount', IntegerType(), True)])) \
        == dshape("{name: string, amount: ?int64}")


def test_dshape_to_schema():
    assert dshape_to_schema('int32') == IntegerType()

    assert dshape_to_schema('5 * int32') == ArrayType(IntegerType(), False)

    assert dshape_to_schema('5 * ?int32') == ArrayType(IntegerType(), True)

    assert dshape_to_schema('{name: string, amount: int32}') == \
        StructType([StructField('name', StringType(), False),
                    StructField('amount', IntegerType(), False)])

    assert dshape_to_schema('10 * {name: string, amount: ?int32}') == \
        ArrayType(StructType(
            [StructField('name', StringType(), False),
             StructField('amount', IntegerType(), True)]),
        False)
