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
        object credentials

    def __init__(self, credentials):
        self.credentials = credentials
        self.q = 0

    def start(self):
        """ given credentials, start the connection to the server """
        cred = self.credentials
        self.q = khpu(cred.host,cred.port,"{0}:{1}".format(cred.username,cred.password))
        if not self.is_initialized:
            raise ValueError("kdb is not initialized: {0}".format(self.q))
        return self

    def stop(self):
        # stop the kdb process
        if self.q:
            kclose(self.q)
        self.q = 0
        return self

    property is_initialized:
        def __get__(self):
            return self.q > 0

    def eval(self, expr, printit=True):
        # pass in an evaluate a q-expression
        # return the result
        cdef:
            K result

        print "\nexpr: {0}".format(expr)
        result = k(self.q,expr)
        print "result.j: {0}".format(result.j)
        print "result.t: {0}, {1}".format(type(result.t),result.t)
        print ','.join([ "{0}:{1}".format(x,y) for x,y in [('m',result.m),('a',result.a),('u',result.u),('r',result.u),('t',result.t)]])

        if (result.t==-128):
           r0(result)
           raise ValueError("server error {0}".format(result.k.s))
        elif (result.t>0):
           # vectorz
           r0(result)
           print "vector received {0}".format(result.t)
        elif (result.t<0):
           print "scalar received {0}".format(result.t)
        else:
           print_k(result)
           #result = kK(result)
           #print_k(result)

           #print "kK[0] -> {0}".format(kK(&result)[0].t)


    #def k_to_frame(self, K k):
        """
        Parameters
        ----------
        k : input K type object of type == XT|XD (table/dict)

        Returns
        -------
        DataFrame

        """
        #assert k.t == XT | k.t == XD

        #cdef:
        #    K col_names, col_data, c
        #    int ncols, nrows, i, j
        #    object name

        # flip if needed
        #k = ktd(k)

        #col_names = kK(flip.k)[0]
        #col_data = kK(flip.k)[1]
        #ncols = col_names->n
        #nrows = kK(col_data)[0]->n

        # iterate the columns
        #for i in range(ncols):

        #    name = kS(col_names)[i]
        #    c = self._convert_column(kK(col_data)[i])

cdef print_k(K x, l=None):
    if l is not None:
        print "{0} -> {1} : {2}".format(l, k_typekind(x), k_itemsize(x))
    else:
        print "{0} : {1}".format(k_typekind(x), k_itemsize(x))
