""" kdb c-level interfaces """


cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

# initialize numpy
import_array()
import_ufunc()

# k.pxd imports
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
        # return the result
        cdef:
            K result

        result = k(self.q,expr,<K>0)

        if (result.t==-128):
           r0(result)
           raise ValueError("server error {0}".format(result.s))
        elif (result.t>0):
           # vectorz
           r0(result)
           print "vector received {0}".format(result.t)
        elif (result.t<0):
           print "scalar received {0}".format(result.t)
        else:
           print_k(result)
           #print_k(kK(result))


           #print "kK[0] -> {0}".format(kK(&result)[0].t)


cdef print_k(K x, l=None):
    if l is not None:
        print "{0} -> {1} : {2}".format(l, k_typekind(x), k_itemsize(x))
    else:
        print "{0} : {1}".format(k_typekind(x), k_itemsize(x))
