#ifndef KX
#define KX
typedef char*S,C;typedef unsigned char G;typedef short H;typedef int I;typedef long long J;typedef float E;typedef double F;typedef void V;
#ifdef __cplusplus
extern"C"{
#endif
#if KXVER>=3
typedef struct k0{signed char m,a,t;C u;I r;union{G g;H h;I i;J j;E e;F f;S s;struct k0*k;struct{J n;G G0[1];};};}*K;
typedef struct{G g[16];}U;
#define kU(x) ((U*)kG(x))
#define xU ((U*)xG)
extern K ku(U),ktn(I,J),kpn(S,J);
#define DO(n,x)	{J i=0,_i=(n);for(;i<_i;++i){x;}}
#else
typedef struct k0{I r;H t,u;union{G g;H h;I i;J j;E e;F f;S s;struct k0*k;struct{I n;G G0[1];};};}*K;
extern K ktn(I,I),kpn(S,I);
#define DO(n,x)	{I i=0,_i=(n);for(;i<_i;++i){x;}}
#endif
#ifdef __cplusplus
}
#endif
//#include<string.h>
// vector accessors, e.g. kF(x)[i] for float&datetime
#define kG(x)	((x)->G0)
#define kC(x)	kG(x)
#define kH(x)	((H*)kG(x))
#define kI(x)	((I*)kG(x))
#define kJ(x)	((J*)kG(x))
#define kE(x)	((E*)kG(x))
#define kF(x)	((F*)kG(x))
#define kS(x)	((S*)kG(x))
#define kK(x)	((K*)kG(x))

//      type bytes qtype     ctype  accessor
#define KB 1  // 1 boolean   char   kG
#define UU 2  // 16 guid     U      kU
#define KG 4  // 1 byte      char   kG
#define KH 5  // 2 short     short  kH
#define KI 6  // 4 int       int    kI
#define KJ 7  // 8 long      long   kJ
#define KE 8  // 4 real      float  kE
#define KF 9  // 8 float     double kF
#define KC 10 // 1 char      char   kC
#define KS 11 // * symbol    char*  kS

#define KP 12 // 8 timestamp long   kJ (nanoseconds from 2000.01.01)
#define KM 13 // 4 month     int    kI (months from 2000.01.01)
#define KD 14 // 4 date      int    kI (days from 2000.01.01)

#define KN 16 // 8 timespan  long   kJ (nanoseconds)
#define KU 17 // 4 minute    int    kI
#define KV 18 // 4 second    int    kI
#define KT 19 // 4 time      int    kI (millisecond)

#define KZ 15 // 8 datetime  double kF (DO NOT USE)

// table,dict
#define XT 98 //   x->k is XD
#define XD 99 //   kK(x)[0] is keys. kK(x)[1] is values.

#ifdef __cplusplus
extern"C"{
#endif
extern I khpun(const S,I,const S,I),khpu(const S,I,const S),khp(const S,I),ymd(I,I,I),dj(I);extern V r0(K),sd0(I),m9(V),kclose(I);extern S sn(S,I),ss(S);
extern K ktj(I,J),ka(I),kb(I),kg(I),kh(I),ki(I),kj(J),ke(F),kf(F),kc(I),ks(S),kd(I),kz(F),kt(I),sd1(I,K(*)(I)),dl(V*f,I),
 knk(I,...),kp(S),ja(K*,V*),js(K*,S),jk(K*,K),k(I,const S,...),xT(K),xD(K,K),ktd(K),r1(K),krr(S),orr(S),dot(K,K),b9(I,K),d9(K);
#ifdef __cplusplus 
}
#endif

// nulls(n?) and infinities(w?)
#define nh ((I)0xFFFF8000)
#define wh ((I)0x7FFF)
#define ni ((I)0x80000000)
#define wi ((I)0x7FFFFFFF)
#ifdef WIN32
#define nj ((J)0x8000000000000000)
#define wj ((J)0x7FFFFFFFFFFFFFFF)
#define nf (log(-1.0))
#define wf (-log(0.0))
#define isnan _isnan
#define finite _finite
extern double log();
#else  
#define nj 0x8000000000000000LL
#define wj 0x7FFFFFFFFFFFFFFFLL
#define nf (0/0.0)
#define wf (1/0.0)
#define closesocket(x) close(x)
#endif 

// remove more clutter
#define O printf
#define R return
#define Z static
#define P(x,y) {if(x)R(y);}
#define U(x) P(!(x),0)
#define SW switch
#define CS(n,x)	case n:x;break;
#define CD default

#define ZV Z V
#define ZK Z K
#define ZH Z H
#define ZI Z I
#define ZJ Z J
#define ZE Z E
#define ZF Z F
#define ZC Z C
#define ZS Z S

#define K1(f) K f(K x)
#define K2(f) K f(K x,K y)
#define TX(T,x) (*(T*)((G*)(x)+8))
#define xr x->r
#define xt x->t
#define xu x->u
#define xn x->n
#define xx xK[0]
#define xy xK[1]
#define xg TX(G,x)
#define xh TX(H,x)
#define xi TX(I,x)
#define xj TX(J,x)
#define xe TX(E,x)
#define xf TX(F,x)
#define xs TX(S,x)
#define xk TX(K,x)
#define xG x->G0
#define xH ((H*)xG)
#define xI ((I*)xG)
#define xJ ((J*)xG)
#define xE ((E*)xG)
#define xF ((F*)xG)
#define xS ((S*)xG)
#define xK ((K*)xG)
#define xC xG
#define xB ((G*)xG)

#endif

