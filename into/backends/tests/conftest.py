import pytest


@pytest.fixture(scope='session')
def sc():
    pyspark = pytest.importorskip('pyspark')
    return pyspark.SparkContext('local', 'into-test')


@pytest.fixture(scope='session')
def sqlctx(sc):
    return pytest.importorskip('pyspark.sql.SQLContext')(sc)
