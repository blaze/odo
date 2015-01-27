from __future__ import absolute_import, division, print_function

import sas7bdat
from sas7bdat import SAS7BDAT

import datashape
from datashape import discover, dshape, var, Record, date_, datetime_
from collections import Iterator
import pandas as pd
from .pandas import coerce_datetimes
from ..append import append
from ..convert import convert
from ..resource import resource


@resource.register('.+\.(sas7bdat)')
def resource_sas(uri, **kwargs):
    return SAS7BDAT(uri, **kwargs)


@discover.register(SAS7BDAT)
def discover_sas(f, **kwargs):
    lines = f.readlines()
    next(lines) # burn header
    ln = next(lines)
    types = map(discover, ln)
    names = [col.name.decode("utf-8") for col in f.header.parent.columns]
    return var * Record(list(zip(names, types)))


@convert.register(pd.DataFrame, SAS7BDAT, cost=4.0)
def sas_to_DataFrame(s, dshape=None, **kwargs):
    df = s.to_data_frame()
    if any(typ in (date_, datetime_) for typ in dshape.measure.types):
        df = coerce_datetimes(df)
    names = [col.decode('utf-8') for col in s.column_names]
    df = df[names]  # Reorder names to match sasfile
    return df


@convert.register(Iterator, SAS7BDAT, cost=1.0)
def sas_to_iterator(s, **kwargs):
    lines = s.readlines()
    if not s.skip_header:
        next(lines)  # burn
    return lines
