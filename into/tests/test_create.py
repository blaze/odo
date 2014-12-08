
from into.create import create
import numpy as np

def test_numpy_create():
    x = create(np.ndarray, dshape='5 * int32')
    assert x.shape == (5,)
    assert x.dtype == 'i4'
