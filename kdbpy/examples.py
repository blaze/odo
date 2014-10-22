""" support examples inteface """

from collections import OrderedDict
import numpy as np
import pandas as pd
from pandas import DataFrame, MultiIndex

class Example(object):
    """ hold an example record, including the q string and the expected result """

    def __init__(self, key, q, result=None):
        self.key = key
        self.q = q
        self.result = result


class Examples(object):
    """ hold q examples for interactive use, serves up a dict-like interface """

    def __init__(self):
        self.data = OrderedDict([ (e.key,e) for e in self.create_data() ])

    def create_data(self):
        """ create the data examples """
        from string import ascii_lowercase

        l = []


        r = list(range(0,10))
        l.append(
            Example('table1',
                    '([]a:til 10;b:reverse til 10;c:`foo;d:{x#.Q.a}each til 10)',
                    DataFrame({'a': r,
                               'b' : list(reversed(r)),
                               'c' : 'foo',
                               'd' : [ ascii_lowercase[0:r1] for r1 in r ],
                               },columns=list('abcd'))))
        l.append(
            Example('table2',
                    'flip `name`iq`fullname!(`Dent`Beeblebrox`Prefect;98 42 126;("Arthur Dent"; "Zaphod Beeblebrox"; "Ford Prefect"))',
                    DataFrame({'iq' : [98, 42, 126],
                               'name' : ['Dent','Beeblebrox','Prefect'],
                               'fullname' : ['Arthur Dent','Zaphod Beeblebrox','Ford Prefect']},
                              columns=['name','iq','fullname'])))

        l.append(
            Example('table3',
                    '([eid:1001 0N 1003;sym:`foo`bar`] pos:`d1`d2`d3;dates:(2001.01.01;2000.05.01;0Nd))',
                    DataFrame({'eid' : [1001, np.nan, 1003],
                               'sym' : ['foo','bar', ''],
                               'pos' : ['d1','d2','d3'],
                               'dates' : pd.to_datetime(['2001-01-01','2000-05-01','nat'])},
                              columns=['eid','sym','pos','dates']).set_index(['eid','sym'])))

        l.extend([
            Example('scalar1','42',42),
            Example('datetime','20130101 10:11:12'),
            ])

        return l

    @property
    def keys(self):
        return self.data.keys()

    @property
    def values(self):
        return self.data.values()

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data.items())
