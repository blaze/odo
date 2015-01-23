from __future__ import absolute_import, division, print_function

import sas7bdat
from sas7bdat import SAS7BDAT

import datashape
from datashape import discover, dshape, var
from collections import Iterator
import pandas as pd
from ..append import append
from ..convert import convert
from ..resource import resource


@resource.register('.+\.(sas7bdat)')
def resource_sas(uri, **kwargs):
    return SAS7BDAT(uri, **kwargs)


@discover.register(SAS7BDAT)
def discover_sas(f, **kwargs):
    f.skip_header = True
    ln = next(f.readlines())
    ds = discover(ln)
    cols = [col.name.decode("utf-8") for col in f.header.parent.columns]
    types = [_type.strip() for _type in str(ds).strip("()").split(',')]
    measure = ",".join(" " + col + ":" + _type for col, _type in zip(cols, types))
    return var * dshape("{" + measure + "}")


@convert.register(pd.DataFrame, SAS7BDAT, cost=4.0)
def sas_to_DataFrame(s, dshape=None, **kwargs):
    return s.to_data_frame()


@convert.register(Iterator, SAS7BDAT, cost=1.0)
def sas_to_iterator(s, **kwargs):
    s.skip_header = True
    return s.readlines()
