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
