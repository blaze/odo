import pytest


@pytest.fixture(scope='session')
def sc():
    pyspark = pytest.importorskip('pyspark')
    return pyspark.SparkContext('local', 'into-test')


@pytest.fixture(scope='session')
def sqlctx(sc):
    sparksql = pytest.importorskip('pyspark.sql')
    return sparksql.SQLContext(sc)
