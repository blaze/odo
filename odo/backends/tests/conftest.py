import os
import shutil
import pytest


@pytest.fixture(scope='session')
def sc():
    pyspark = pytest.importorskip('pyspark')
    return pyspark.SparkContext('local[*]', 'odo')


@pytest.yield_fixture(scope='session')
def sqlctx(sc):
    pyspark = pytest.importorskip('pyspark')
    try:
        if hasattr(pyspark.sql, 'types'):
            yield pyspark.HiveContext(sc)
        else:
            yield pyspark.SQLContext(sc)
    finally:
        dbpath = 'metastore_db'
        logpath = 'derby.log'
        if os.path.exists(dbpath):
            assert os.path.isdir(dbpath)
            shutil.rmtree(dbpath)
        if os.path.exists(logpath):
            assert os.path.isfile(logpath)
            os.remove(logpath)
