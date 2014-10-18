""" kdb c-level interfaces """


cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

# initialize numpy
import_array()
import_ufunc()

from k cimport *

cdef class KDB:
    """ represents the c-level interface to the kdb/q processes """

    cdef:
        int q

    def __init__(self, cred):
        # given credentials, start the connection to the server

        self.q = khpu(cred.host,cred.port,"{0}:{1}".format(cred.username,cred.password))
        if not self.is_initialized:
            raise ValueError("kdb is not initialized: {0}".format(self.q))

    def close(self):
        # close the kdb process

        if self.q:
            kclose(self.q)
        self.q = 0

    property is_initialized:

        def __get__(self):
            return self.q > 0

    def eval(self, expr):
        # pass in an evaluate a q-expression
        k(self.q,expr)

    def get_memory_used(self):
        k(self.q,'.Q.w[]')