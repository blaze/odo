from into.backends.bcolz import create, append, convert, ctable, carray
import numpy as np

def test_convert():
    assert isinstance(convert(carray, np.ones([1, 2, 3])), carray)
    b = carray([1, 2, 3])
    assert isinstance(convert(np.ndarray, b), np.ndarray)
