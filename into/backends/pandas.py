from __future__ import absolute_import, division, print_function

from datashape import discover
import datashape

import pandas as pd

@discover.register(pd.DataFrame)
def discover_dataframe(df):
    obj = datashape.coretypes.object_
    names = list(df.columns)
    dtypes = list(map(datashape.CType.from_numpy_dtype, df.dtypes))
    dtypes = [datashape.string if dt == obj else dt for dt in dtypes]
    schema = datashape.Record(list(zip(names, dtypes)))
    return len(df) * schema


@discover.register(pd.Series)
def discover_series(s):
    return len(s) * datashape.CType.from_numpy_dtype(s.dtype)
