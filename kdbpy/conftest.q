n: 100
m: 200
ts: ([] ts: 00:00:00.000000000 + 1 + til 5; amount: til 5)
date_t: ([] d: 2014.01.01 + til 5; ts: 2014.01.01D00:00:00.000000000 + til 5)
dates: ([] account: n ? `bofa`citi`capone; date: n ? (2010.01.01 + til 4))
prices: ([] account: m ? `bofa`citi`capone; amount: m ? 100000.0)
k: 6
t: ([] name: `Bob`Alice`Joe`Smithers`Smithers`Alice;
       id: til k;
       amount: k ? 10.0;
       when: 2003.12.05D22:23:12
             2005.03.04D03:24:45.514
             2005.02.28D01:58:04.338
             2004.01.25D18:03:47.234
             2005.02.28D01:58:04.338
             2004.01.25D18:03:47.234;
       on: 2010.01.01 2010.01.02 2010.01.03 2010.01.04 2010.01.04 2010.01.02)
rt: ([name: `Bob`Alice`Joe`John] tax: -3.1 2.0 0n 4.2;
                                 street: `maple`apple`pine`grove)
st: ([name: `Bob`Alice`Joe] jobcode: 9 10 11; tree: `maple`apple`pine; alias: `Joe`Betty`Moe)
kt: ([house: `a`b`c; id: 1 2 3] amount: 3.0 4.0 5.0)

exchange: ([m_id: til 3] name: `DJIA`NYSE`NDX)
market: ([] ex_id: `exchange$(); volume: `long$())
`market insert (10 ? til 3; 10 ? 1000); // don't show output when loading
