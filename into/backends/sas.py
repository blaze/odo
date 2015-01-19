from __future__ import absolute_import, division, print_function

import sas7bdat
from sas7bdat import SAS7BDAT
import datashape
from datashape import discover

import re
import datashape
from datashape import discover, Record, Option, dshape
from datashape.predicates import isrecord
from datashape.dispatch import dispatch
from collections import Iterator
import pandas
import pandas as pd
import os
import gzip
import bz2

from ..utils import keywords
from ..append import append
from ..convert import convert
from ..resource import resource
from ..chunks import chunks
from ..numpy_dtype import dshape_to_pandas
from .pandas import coerce_datetimes

SAS_type_map = {'number': 'float64',
                'string': 'string'}


@resource.register('.+\.(sas7bdat)')
def resource_csv(uri, **kwargs):
    return SAS7BDAT(uri, **kwargs)


@discover.register(SAS7BDAT)
def discover_sas(f, **kwargs):
    cols = [col.name.decode("utf-8") for col in f.header.parent.columns]
    types = [SAS_type_map[col.type] for col in f.header.parent.columns]
    measure = ",".join(col + ":" + _type for col, _type in zip(cols, types))
    ds = "var * {" + measure + "}"
    return dshape(ds)


@convert.register(pd.DataFrame, SAS7BDAT, cost=1.0)
def sas_to_DataFrame(s, dshape=None, **kwargs):
    return s.to_data_frame()


@convert.register(list, SAS7BDAT, cost=1.0)
def sas_to_list(s, dshape=None, **kwargs):
    return list(s.readlines())


@convert.register(Iterator, SAS7BDAT, cost=1.0)
def sas_to_iterator(s, columns=None, dshape=None, **kwargs):
    return s.readlines()
