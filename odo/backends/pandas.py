from __future__ import absolute_import, division, print_function

import numpy as np
from datashape import discover
from datashape import float32, float64, string, Option, object_, datetime_
import datashape

import pandas as pd

from toolz import curry


possibly_missing = set((string, datetime_, float32, float64))


@discover.register(pd.DataFrame)
def discover_dataframe(df):
    obj = object_
    names = list(df.columns)
    dtypes = list(map(datashape.CType.from_numpy_dtype, df.dtypes))
    dtypes = [string if dt == obj else dt for dt in dtypes]
    odtypes = [Option(dt) if dt in possibly_missing else dt for dt in dtypes]
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
    converter = curry(pd.to_datetime, infer_datetime_format=True)
    df2 = df.select_dtypes(include=['object']).apply(converter)
    datetime64_ns = np.dtype('datetime64[ns]')
    object_dype = np.dtype('object')
    for c in df2.columns:
        # dateutil converts things like Sun to the date of the next upcoming
        # Sunday so only assign if the type has changed but shouldn't have
        if not (df2[c].dtype == datetime64_ns and
                df[c].dtype == object_dype and
                (df[c].str.isalpha() | df[c].str.isspace()).any()):
            df[c] = df2[c]
    return df
