import pytest
import numpy as np
from kdbpy import lib

def test_kdb_construction():

    kdb = lib.KDB()
    assert not kdb.is_initialized
    kdb.initialize()
    assert kdb.is_initialized
    kdb.close()
    assert not kdb.is_initialized

def test_kdb_evaluate():

    kdb = lib.KDB()

    # require initilization
    with pytest.raises(ValueError):
        kdb.evaluate('float64')

    kdb.initialize()
    result = kdb.evaluate('float64')
    assert np.array_equal(result, np.zeros(10,dtype='float64'))
    kdb.close()
