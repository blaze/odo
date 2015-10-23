from __future__ import absolute_import, division, print_function

from datetime import datetime
from functools import partial

from datashape import discover
from datashape import float32, float64, string, Option, object_, datetime_
import datashape

import pandas as pd
import numpy as np

from ..convert import convert


possibly_missing = set((string, datetime_, float32, float64))


@discover.register(pd.DataFrame)
def discover_dataframe(df):
    obj = object_
    names = list(df.columns)
    dtypes = list(map(datashape.CType.from_numpy_dtype, df.dtypes))
    dtypes = [string if dt == obj else dt for dt in dtypes]
    odtypes = [Option(dt) if dt in possibly_missing else dt
               for dt in dtypes]
    schema = datashape.Record(list(zip(names, odtypes)))
    return len(df) * schema


@discover.register(pd.Series)
def discover_series(s):
    return len(s) * datashape.CType.from_numpy_dtype(s.dtype)


def coerce_datetimes(df):
    """ Make object columns into datetimes if possible

    Warning: this operates inplace.

    Example
    -------

    >>> df = pd.DataFrame({'dt': ['2014-01-01'], 'name': ['Alice']})
    >>> df.dtypes  # note that these are strings/object
    dt      object
    name    object
    dtype: object

    >>> df2 = coerce_datetimes(df)
    >>> df2
              dt   name
    0 2014-01-01  Alice

    >>> df2.dtypes  # note that only the datetime-looking-one was transformed
    dt      datetime64[ns]
    name            object
    dtype: object
    """

    def patched_to_datetime(s):
        # NOTE: In pandas < 0.17, pd.to_datetime(' ') == datetime(...), which
        # is not what we want.  This patched version detects empty or
        # whitespace-only strings, and maps them to a dummy object.  This
        # prevents datetime coercion on whitespace or empty strings.
        o = object()
        s_norm = s.apply(lambda x: o if (hasattr(x, 'strip') and not x.strip()) else x)
        res = pd.to_datetime(s_norm, errors='ignore')
        return res.apply(lambda x: '' if x is o else x)

    df2 = df.select_dtypes(include=['object']).apply(patched_to_datetime)
    for c in df2.columns:
        df[c] = df2[c]
    return df


@convert.register(pd.Timestamp, datetime)
def convert_datetime_to_timestamp(dt, **kwargs):
    return pd.Timestamp(dt)


@convert.register(pd.Timestamp, float)
def nan_to_nat(fl, **kwargs):
    try:
        if np.isnan(fl):
            # Only nan->nat edge
            return pd.NaT
    except TypeError:
        pass
    raise NotImplementedError()


@convert.register(pd.Timestamp, (pd.tslib.NaTType, type(None)))
def convert_null_or_nat_to_nat(n, **kwargs):
    return pd.NaT
