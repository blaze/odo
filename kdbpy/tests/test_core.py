import pytest
import numpy as np
from kdbpy import lib

def test_kdb_construction():

    kdb = lib.KDB()
    assert kdb.is_initialized
    kdb.close()
    assert not kdb.is_initialized

    # require initilization
    with pytest.raises(ValueError):
        lib.KDB(port=0)

def test_kdb_evaluate():

    kdb = lib.KDB()
    result = kdb.evaluate("a:42")
    kdb.close()
