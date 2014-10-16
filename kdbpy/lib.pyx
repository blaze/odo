cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

# initialize numpy
import_array()
import_ufunc()

cdef class KDB:

    cdef:
        object q

    def __init__(self, q=None):
        self.q = q

    def initialize(self):
        self.q = True
        return self

    def close(self):
        self.q = None

    property is_initialized:

        def __get__(self):
            return self.q is not None

    def evaluate(self, expr):
        cdef ndarray result

        if not self.is_initialized:
            raise ValueError("q is not initialized")

        result = np.zeros(10,dtype=expr)
        return result
