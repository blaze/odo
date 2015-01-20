from __future__ import absolute_import, division, print_function

import sas7bdat
from sas7bdat import SAS7BDAT

import datashape
from datashape import discover, dshape
from collections import Iterator
import pandas as pd
import sqlalchemy as sa
from .sql import dshape_to_alchemy, dshape_to_table

from ..append import append
from ..convert import convert
from ..resource import resource

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


@convert.register(pd.DataFrame, SAS7BDAT, cost=4.0)
def sas_to_DataFrame(s, dshape=None, **kwargs):
    return s.to_data_frame()


@convert.register(list, SAS7BDAT, cost=8.0)
def sas_to_list(s, dshape=None, **kwargs):
    s.skip_header = True
    return list(s.readlines())


@convert.register(Iterator, SAS7BDAT, cost=1.0)
def sas_to_iterator(s):
    s.skip_header = True
    return s.readlines()


@append.register(sa.Table, SAS7BDAT)
def append_sas_to_table(t, s, **kwargs):
    append(t, sas_to_iterator(s), **kwargs)


def sas_to_table(s, metadata=None):
    ds = discover_sas(s)
    name = s.header.properties.name.decode("utf-8")
    return dshape_to_table(name, ds, metadata)
