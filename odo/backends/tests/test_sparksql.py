from __future__ import print_function, absolute_import, division

import pytest

pyspark = pytest.importorskip('pyspark')
py4j = pytest.importorskip('py4j')

import os
import shutil
import json
import tempfile
from contextlib import contextmanager

import toolz
from toolz.compatibility import map

from pyspark.sql import Row
from pyspark.sql.types import (
    ArrayType, StructField, StructType, IntegerType, StringType
)

try:
    from pyspark.sql.utils import AnalysisException
except ImportError:
    AnalysisException = py4j.protocol.Py4JJavaError

import numpy as np
import pandas as pd
import pandas.util.testing as tm

import datashape
from datashape import dshape
from odo import odo, discover, Directory, JSONLines
from odo.utils import tmpfile, ignoring
from odo.backends.sparksql import schema_to_dshape, dshape_to_schema
from odo.backends.sparksql import SparkDataFrame


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
    try:
        df = sqlctx.createDataFrame(people)
    except AttributeError:
        schema = sqlctx.inferSchema(people)
        schema.registerTempTable('t')
        schema.registerTempTable('t2')
    else:
        df2 = sqlctx.createDataFrame(people)
        sqlctx.registerDataFrameAsTable(df, 't')
        sqlctx.registerDataFrameAsTable(df2, 't2')
    return sqlctx


def test_pyspark_to_sparksql(ctx, people):
    sdf = odo(data, ctx, dshape=discover(df))
    assert isinstance(sdf, SparkDataFrame)
    assert (list(map(set, odo(people, list))) ==
            list(map(set, odo(sdf, list))))


def test_pyspark_to_sparksql_raises_on_tuple_dshape(ctx, people):
    with pytest.raises(TypeError):
        odo(data, ctx)


def test_dataframe_to_sparksql(ctx):
    sdf = odo(df, ctx)
    assert isinstance(sdf, SparkDataFrame)
    assert odo(sdf, list) == odo(df, list)


def test_sparksql_to_frame(ctx):
    result = odo(ctx.table('t'), pd.DataFrame)
    np.testing.assert_array_equal(result.sort_index(axis=1).values,
                                  df.sort_index(axis=1).values)


def test_reduction_to_scalar(ctx):
    result = odo(ctx.sql('select sum(amount) from t'), float)
    assert isinstance(result, float)
    assert result == sum(map(toolz.second, data))


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


def test_append_spark_df_to_json_lines(ctx):
    out = os.linesep.join(map(json.dumps, df.to_dict('records')))
    sdf = ctx.table('t')
    expected = pd.concat([df, df]).sort('amount').reset_index(drop=True).sort_index(axis=1)
    with tmpfile('.json') as fn:
        with open(fn, mode='w') as f:
            f.write(out + os.linesep)

        uri = 'jsonlines://%s' % fn
        odo(sdf, uri)
        result = odo(uri, pd.DataFrame).sort('amount').reset_index(drop=True).sort_index(axis=1)
        tm.assert_frame_equal(result, expected)


@pytest.mark.xfail(raises=(py4j.protocol.Py4JJavaError, AnalysisException),
                   reason='bug in sparksql')
def test_append(ctx):
    """Add support for odo(SparkDataFrame, SparkDataFrame) when this is fixed.
    """
    a = ctx.table('t2')
    a.insertInto('t2')
    result = odo(odo(a, pd.DataFrame), set)
    expected = odo(pd.concat([odo(a, pd.DataFrame)]) * 2, set)
    assert result == expected


def test_load_from_jsonlines(ctx):
    with tmpfile('.json') as fn:
        js = odo(df, 'jsonlines://%s' % fn)
        result = odo(js, ctx, name='r')
        assert (list(map(set, odo(result, list))) ==
                list(map(set, odo(df, list))))


@contextmanager
def jslines(n=3):
    d = tempfile.mkdtemp()
    files = []
    dfc = df.copy()
    for i in range(n):
        _, fn = tempfile.mkstemp(suffix='.json', dir=d)
        dfc['id'] += i
        odo(dfc, 'jsonlines://%s' % fn)
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
        result = odo(Directory(JSONLines)(d), ctx)
        assert (set(map(frozenset, odo(result, list))) ==
                set(map(frozenset, odo(expected, list))))
