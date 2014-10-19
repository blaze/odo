cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

cdef extern from "k.h":

   # basic data types
   ctypedef char *S
   ctypedef char  C
   ctypedef unsigned char G
   ctypedef short H
   ctypedef int I
   ctypedef long long J
   ctypedef float E
   ctypedef double F
   ctypedef void V

   # primary K structure
   cdef struct k0:
       signed char m
       signed char a
       signed char t # type
       C u
       I r # memory references
       G g # unsigned char
       H h # short
       I i # int
       J j # long long
       E e # float
       F f # double
       S s # symbol
       k0 *k # pointer to a K
       J n # long long
       G G0[1] # array of unsigned char
   ctypedef k0 *K

   # vector accessors
   cdef :
      G kG(K x)
      C kC(K x)
      H *kH(K *x)
      I *kI(K *x)
      J *kJ(K *x)
      E *kE(K *x)
      F *kF(K *x)
      S *kS(K *x)
      K *kK(K *x)

   # scalar accessors
   cdef :
      K ka(I) # atom
      K kb(I) # boolean
      K ku(U) # guid
      K kg(I) # byte
      K kh(I) # short
      K ki(I) # int
      K kj(J) # long
      K ke(F) # real
      K kf(F) # float
      K kc(I) # char
      K ks(S) # symbol
      K ktj(KP,J) # timestamp
      K kt(I) # time
      K kd(I) # date
      K ktj(KN,J) # timespan
      K kz(F) # datetime

   cdef :
       I khpun(const S,I,const S,I)
       I khpu(const S,I,const S)
       I khp(const S,I)
       I okx(K)
       I ymd(I,I,I)
       I dj(I)
       V r0(K)
       V sd0(I)
       V kclose(I)
       S sn(S,I)
       S ss(S)
       K ktj(I,J)
       K ka(I)
       K kb(I)
       K kg(I)
       K kh(I)
       K ki(I)
       K kj(J)
       K ke(F)
       K kf(F)
       K kc(I)
       K ks(S)
       K kd(I)
       K kz(F)
       K kt(I)
       K sd1(I,K(*)(I))
       K dl(V*f,I)
       K knk(I,...)
       K kp(S)
       K ja(K*,V*)
       K js(K*,S)
       K jk(K*,K)
       K jv(K*k,K)
       K k(I,const S,...)
       K xT(K)
       K xD(K,K)
       K ktd(K)
       K r1(K)
       K krr(const S)
       K orr(const S)
       K dot(K,K)
       K b9(I,K)
       K d9(K)

   # type definitions
   cdef enum K_TYPES:
       KB #  1  // 1 boolean   char   kG
       UU #  2  // 16 guid     U      kU
       KG #  4  // 1 byte      char   kG
       KH #  5  // 2 short     short  kH
       KI #  6  // 4 int       int    kI
       KJ #  7  // 8 long      long   kJ
       KE #  8  // 4 real      float  kE
       KF #  9  // 8 float     double kF
       KC # 10 // 1 char      char   kC
       KS # 11 // * symbol    char*  kS

       KP # 12 // 8 timestamp long   kJ (nanoseconds from 2000.01.01)
       KM # 13 // 4 month     int    kI (months from 2000.01.01)
       KD # 14 // 4 date      int    kI (days from 2000.01.01)

       KN # 16 // 8 timespan  long   kJ (nanoseconds)
       KU # 17 // 4 minute    int    kI
       KV # 18 // 4 second    int    kI
       KT # 19 // 4 time      int    kI (millisecond)

       KZ # 15 // 8 datetime  double kF (DO NOT USE)

       # // table,dict
       XT # 98 //   x->k is XD
       XD # 99 //   kK(x)[0] is keys. kK(x)[1] is values.

cdef inline char *k_typekind(K x):

     cdef int t = abs(x.t);
     cdef k_typechars = "ObXXuiiiffSOXiifXiii"

     if (t < len(k_typechars)):
          return k_typechars[t]
     return 'X'

cdef inline int k_itemsize(K x):
     cdef int t = abs(x.t);
     cdef int *k_itemsizes = [
                sizeof(void*),
                1, # bool
                16,
                0, 1, # byte
                2, # short
                4, # int
                8, # long
                4, # float
                8, # real
                1, # char
                sizeof(void*), # symbol
                0, 4, # month
                4, # date
                8, # datetime
                0, 4, # minute
                4, # second
                4, # time
     ]

     if (t < sizeof(k_itemsizes)):
           return k_itemsizes[t]
     return 0
