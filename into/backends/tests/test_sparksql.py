from __future__ import print_function, absolute_import, division

import pytest

pyspark = pytest.importorskip('pyspark')

import shutil
import tempfile
from contextlib import contextmanager

from pyspark.sql import SchemaRDD, Row
from pyspark.sql import ArrayType, StructField, StructType, IntegerType
from pyspark.sql import StringType

import numpy as np
import pandas as pd

import datashape
from datashape import dshape
from into import into, discover, Directory, JSONLines
from into.utils import tmpfile, ignoring
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
    sdf = into(ctx, data, dshape=discover(df))
    assert isinstance(sdf, SchemaRDD)
    assert (list(map(set, into(list, people))) ==
            list(map(set, into(list, sdf))))


def test_pyspark_to_sparksql_raises_on_tuple_dshape(ctx, people):
    with pytest.raises(TypeError):
        into(ctx, data)


def test_dataframe_to_sparksql(ctx):
    sdf = into(ctx, df)
    assert isinstance(sdf, SchemaRDD)
    assert into(list, sdf) == into(list, df)


def test_sparksql_to_frame(ctx):
    result = into(pd.DataFrame, ctx.table('t'))
    np.testing.assert_array_equal(result.sort_index(axis=1).values,
                                  df.sort_index(axis=1).values)


def test_discover_context(ctx):
    result = discover(ctx)
    assert result is not None


def test_schema_to_dshape():
    assert schema_to_dshape(IntegerType()) == datashape.int32

    assert schema_to_dshape(
        ArrayType(IntegerType(), False)) == dshape("var * int32")

    assert schema_to_dshape(
        ArrayType(IntegerType(), True)) == dshape("var * ?int32")

    assert schema_to_dshape(StructType([
        StructField('name', StringType(), False),
        StructField('amount', IntegerType(), True)])) \
        == dshape("{name: string, amount: ?int32}")


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


def test_load_from_jsonlines(ctx):
    with tmpfile('.json') as fn:
        js = into('jsonlines://%s' % fn, df)
        result = into(ctx, js, name='r')
        assert (list(map(set, into(list, result))) ==
                list(map(set, into(list, df))))


@contextmanager
def jslines(n=3):
    d = tempfile.mkdtemp()
    files = []
    dfc = df.copy()
    for i in range(n):
        _, fn = tempfile.mkstemp(suffix='.json', dir=d)
        dfc['id'] += i
        into('jsonlines://%s' % fn, dfc)
        files.append(fn)

    yield d

    with ignoring(OSError):
        shutil.rmtree(d)


def test_load_from_dir_of_jsonlines(ctx):
    dfs = []
    dfc = df.copy()
    for i in range(3):
        dfc['id'] += i
        dfs.append(dfc.copy())
    expected = pd.concat(dfs, axis=0, ignore_index=True)
    with jslines() as d:
        result = into(ctx, Directory(JSONLines)(d))
        assert (set(map(frozenset, into(list, result))) ==
                set(map(frozenset, into(list, expected))))
