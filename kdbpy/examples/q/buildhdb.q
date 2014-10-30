/ buildhdb.q
/ builds a historical trade/quote database
//
/ tables:
/ daily: date sym open high low close price size
/ depth: date time sym price size side ex
/ mas: sym name
/ nbbo: date time sym bid ask bsize asize
/ quote: date time sym bid ask bsize asize mode ex
/ trade: date time sym price size stop cond ex
//
/ trade, quote, nbbo are partitioned by date
/ round robin partition is optional
//
/ requires write permission in target directories

/ config
dst:`:start/db      / database root
dsp:""              / optional database segment root
dsx:5              / number of segments

bgn:2013.05.01      / begin, end
end:2013.05.31      / (only mon-fri used)

/ approximate values:
nt:100            / trades per stock per day
qpt:5              / quotes per trade
npt:3              / nbbo per trade
nl2:100            / level II entries in day

\S 104831          / random seed
/ util

pi:acos -1
accum:{prds 1.0,-1 _ x}
int01:{(til x)%x-1}
limit:{(neg x)|x & y}
minmax:{(min x;max x)}
normalrand:{(cos 2 * pi * x ? 1f) * sqrt neg 2 * log x ? 1f}
rnd:{0.01*floor 0.5+x*100}
xrnd:{exp x * limit[2] normalrand y}
randomize:{value "\\S ",string "i"$0.8*.z.p%1000000000}
shiv:{(last x)&(first x)|asc x+-2+(count x)?5}
vol:{10+`int$x?90}
vol2:{x?100*1 1 1 1 2 2 3 4 5 8 10 15 20}

/ =========================================================
choleski:{
 n:count A:x+0.0;
 if[1>=n;:sqrt A];
 p:ceiling n%2;
 X:p#'p#A;
 Y:p _'p#A;
 Z:p _'p _A;
 T:(flip Y) mmu inv X;
 L0:n #' (choleski X) ,\: (n-1)#0.0;
 L1:choleski Z-T mmu Y;
 L0,(T mmu p#'L0),'L1}

/ =========================================================
/ paired correlation, matrix of variates, min 0.1 coeff
choleskicor:{
 x:"f"$x;y:"f"$y;
 n:count y;
 c:0.1|(n,n)#1.0,x,((n-2)#0.0),x;
 (choleski c) mmu y}

/ =========================================================
/ volume profile - random times, weighted toward ends
/ x=count
volprof:{
 p:1.75;
 c:floor x%3;
 b:(c?1.0) xexp p;
 e:2-(c?1.0) xexp p;
 m:(x-2*c)?1.0;
 {(neg count x)?x} m,0.5*b,e}

/ =========================================================
write:{
 t:.Q.en[dst] update sym:`p#sym from `sym xasc y;
 $[count dsp;
  (` sv dsp,(`$"d",string dspx),`$x) set t;
  (` sv dst,`$x) set t];}
/ symbol data for tick demo

sn:2 cut (
 `AMD;"ADVANCED MICRO DEVICES";
 `AIG;"AMERICAN INTL GROUP INC";
 `AAPL;"APPLE INC COM STK";
 `DELL;"DELL INC";
 `DOW;"DOW CHEMICAL CO";
 `GOOG;"GOOGLE INC CLASS A";
 `HPQ;"HEWLETT-PACKARD CO";
 `INTC;"INTEL CORP";
 `IBM;"INTL BUSINESS MACHINES CORP";
 `MSFT;"MICROSOFT CORP";
 `ORCL;"ORACLE CORPORATION";
 `PEP;"PEPSICO INC";
 `PRU;"PRUDENTIAL FINANCIAL INC.";
 `SBUX;"STARBUCKS CORPORATION";
 `TXN;"TEXAS INSTRUMENTS")

s:first each sn
n:last each sn
p:33 27 84 12 20 72 36 51 42 29 35 22 59 63 18
m:" ABHILNORYZ" / mode
c:" 89ABCEGJKLNOPRTWZ" / cond
e:"NONNONONNNNOONO" / ex
/ gen

vex:1.0005        / average volume growth per day
ccf:0.5            / correlation coefficient

/ =========================================================
/ qx index, qb/qbb/qa/qba margins, qp price, qn position
batch:{[x;len]
  p0:prices[;x];
  p1:prices[;x+1];
  d:xrnd[0.0003] len;
  qx::len?cnt;
  qb::rnd len?1.0;
  qa::rnd len?1.0;
  qbb::qb & -0.02 + rnd len?1.0;
  qba::qa & -0.02 + rnd len?1.0;
  n:where each qx=/:til cnt;
  s:p0*accum each d n;
  s:s + (p1-last each s)*{int01 count x} each s;
  qp::len#0.0;
  (qp n):rnd s;
  qn::0}

/ =========================================================
/ constrained random walk
/ x max movement per step
/ y max movement at any time (above/below)
/ z number of steps
cgen:{
  m:reciprocal y;
  while[any (m>p) or y<p:prds 1.0+x*normalrand z];
  p}

/ =========================================================
getdates:{
 b:x 0;
 e:x 1;
 d:b + til 1 + e-b;
 d:d where 5> d-`week$d;
 hols:101 404 612 701 1001 1013 1225 1226;
 d where not ((`dd$d)+100*`mm$d) in hols}

/ =========================================================
makeprices:{
 r:cgen[0.0375;3] each cnt#nd;
 r:choleskicor[ccf;1,'r];
 (p % first each r) * r *\: 1.1 xexp int01 nd+1}

/ =========================================================
/ day volumes
makevolumes:{
 v:cgen[0.03;3;x];
 a:vex xexp neg x;
 0.05|2&v*a+((reciprocal last v)-a)*int01 x}
/ main

cnt:count s
dates:getdates bgn,end
nd:count dates
td:([]date:();sym:();open:();high:();low:();close:();price:();size:())

prices:makeprices nd + 1
volumes:floor (cnt*nt*qpt+npt) * makevolumes nd
dspx:0
patt:{update sym:`p#sym from `sym`time xasc x}


day:{
  len:volumes x;
  batch[x;len];
  sa:string dx:dates x;
  r:asc 09:30:00.0+floor 23400000*volprof count qx;
  cx:len?qpt+npt;
  cn:count n:where cx=0;
  sp:1=cn?20;
  t:([]sym:s qx n;time:shiv r n;price:qp n;size:vol cn;stop:sp;cond:cn?c;ex:e qx n);
  tx:select open:first price,high:max price,low:min price,close:last price,price:sum price*size,sum size by sym from t;
  td,:([]date:(count s)#dx)+0!tx;
  cn:count n:where cx<qpt;
  q:([]sym:s qx n;time:r n;bid:(qp-qb)n;ask:(qp+qa)n;bsize:vol cn;asize:vol cn;mode:cn?m;ex:e qx n);
  cn:count n:where cx>=qpt;
  b:([]sym:s qx n;time:r n;bid:(qp-qbb)n;ask:(qp+qba)n;bsize:vol cn;asize:vol cn);
  write[sa,"/trade/";t];
  write[sa,"/quote/";q];
  dspx::(dspx+1) mod dsx;}

day each til nd;
nbbo_day:{
  len:volumes x;
  batch[x;len];
  sa:string dx:dates x;
  r:asc 09:30:00.0+floor 23400000*volprof count qx;
  cx:len?qpt+npt;
  cn:count n:where cx=0;
  sp:1=cn?20;
  t:([]sym:s qx n;time:shiv r n;price:qp n;size:vol cn;stop:sp;cond:cn?c;ex:e qx n);
  tx:select open:first price,high:max price,low:min price,close:last price,price:sum price*size,sum size by sym from t;
  td,:([]date:(count s)#dx)+0!tx;
  cn:count n:where cx<qpt;
  q:([]sym:s qx n;time:r n;bid:(qp-qb)n;ask:(qp+qa)n;bsize:vol cn;asize:vol cn;mode:cn?m;ex:e qx n);
  cn:count n:where cx>=qpt;
  b:([]sym:s qx n;time:r n;bid:(qp-qbb)n;ask:(qp+qba)n;bsize:vol cn;asize:vol cn);
  b}

nbbo_t: (,) over (nbbo_day each til nd);

{
  batch[nd-1;nl2];
  r:asc 09:30:00.0+(count qx)?28800000;
  m:nl2?2;
  t:([]date:last dates;time:r;sym:s qx;price:qp+qa*-1 1 m;size:vol2 nl2;side:"BS" m;ex:e qx);
  (` sv dst,`depth) set .Q.en[dst] t;}[];

(` sv dst,`daily) set .Q.en[dst] td;
(` sv dst,`mas) set .Q.en[dst] ([]sym:s;name:n);
`:./start/db/nbbo_t/ set .Q.en[dst] nbbo_t;
if[count dsp;(` sv dst,`par.txt) 0: ((1_string dsp),"/d") ,/: string til dsx];
