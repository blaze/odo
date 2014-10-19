cimport numpy as np
cimport cython
import numpy as np

from numpy cimport *
cimport cpython

cdef extern from "k.h":
   ctypedef char *S
   ctypedef char  C
   ctypedef unsigned char G
   ctypedef short H
   ctypedef int I
   ctypedef long long J
   ctypedef float E
   ctypedef double F
   ctypedef void V

   cdef struct k0:
       signed char m
       signed char a
       signed char t
       C u
       I r
       G g
       H h
       I i
       J j
       E e
       F f
       S s
       k0 *k
       J n
       G G0[1]
   ctypedef k0 K

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
