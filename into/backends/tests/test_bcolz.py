from into.backends.bcolz import create, append, convert, ctable, carray
from into.chunks import chunks
import numpy as np


def eq(a, b):
    c = a == b
    if isinstance(c, np.ndarray):
        c = c.all()
    return c


a = carray([1, 2, 3, 4])
x = np.array([1, 2])


def test_convert():
    assert isinstance(convert(carray, np.ones([1, 2, 3])), carray)
    b = carray([1, 2, 3])
    assert isinstance(convert(np.ndarray, b), np.ndarray)


def test_chunks():
    c = convert(chunks(np.ndarray), a)
    assert isinstance(c, chunks(np.ndarray))

    assert eq(convert(np.ndarray, c), a[:])


def test_append_chunks():
    b = carray(x)

    append(b, chunks(np.ndarray)([x, x]))

    assert len(b) == len(x) * 3
