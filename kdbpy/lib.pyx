cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

from k cimport *

# initialize numpy
import_array()
import_ufunc()

cdef class KDB:

    cdef:
        int kdb

    def __init__(self, host='localhost', port=5001):
        self.kdb = khp(host,port)
        if not self.is_initialized:
            raise ValueError("kdb is not initialized")

    def close(self):
        if self.kdb:
            kclose(self.kdb)
        self.kdb = 0

    property is_initialized:

        def __get__(self):
            return self.kdb > 0

    def evaluate(self, expr):
        # pass in an evaluate a q-expression
        k(self.kdb,expr)

    def get_memory_used(self):
        k(self.kdb,'.Q.w[]')