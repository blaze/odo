from __future__ import absolute_import, division, print_function

import array
import uuid
from datetime import datetime, date

import pandas as pd

from toolz import merge, keyfilter
from toolz.compatibility import zip, map
from cytoolz import take

from graphlab import SFrame, SArray

from datashape import (string, int64, float64, var, Record, Option, datetime_,
                       date_)

from ..convert import convert
from .csv import CSV
from ..temp import Temp
from ..utils import keywords
from .. import discover


__all__ = ['SFrame', 'SArray']


python_type_to_datashape = {
    str: string,
    int: int64,
    float: float64,
    datetime: datetime_,
    date: date_
}


@discover.register(SFrame)
def discover_sframe(sf, n=1000):
    columns = sf.column_names()
    types = map(lambda x, n=n: discover(x, n=n).measure,
                (sf[name] for name in columns))
    return var * Record(list(zip(columns, types)))


@discover.register(SArray)
def discover_sarray(sa, n=1000):
    dtype = sa.dtype()
    if issubclass(dtype, (dict, list, array.array)):
        measure = discover(list(take(n, sa))).measure
    else:
        measure = Option(python_type_to_datashape[dtype])
    return var * measure


@convert.register(pd.DataFrame, SFrame, cost=5.0)
def convert_sframe_to_dataframe(sf, **kwargs):
    return sf.to_dataframe()


@convert.register(pd.Series, SArray, cost=5.0)
def convert_sframe_to_dataframe(sa, **kwargs):
    return pd.Series(sa)


@convert.register(SFrame, CSV, cost=10.0)
def convert_csv_to_sframe(csv, **kwargs):
    kwd_names = set(keywords(SFrame.read_csv))
    dialect = merge(csv.dialect, keyfilter(kwd_names.__contains__, kwargs))
    return SFrame.read_csv(csv.path,
                           delimiter=dialect.pop('delimiter', ','),
                           header=dialect.pop('header', bool(csv.has_header)),
                           escape_char=dialect.pop('escapechar', '\\'),
                           double_quote=dialect.pop('doublequote', True),
                           quote_char=dialect.pop('quotechar', '"'),
                           skip_initial_space=dialect.pop('skipinitialspace',
                                                          True),
                           **dialect)


@convert.register(Temp(CSV), SFrame)
def convert_sframe_to_temp_csv(sf, dshape=None, **kwargs):
    assert not kwargs, 'no keyword arguments allowed'
    filename = '.%s' % uuid.uuid1()
    sf.save(filename, format='csv')
    return Temp(CSV)(filename)
