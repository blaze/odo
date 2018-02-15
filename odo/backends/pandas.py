from __future__ import absolute_import, division, print_function

from datetime import datetime, timedelta
from functools import partial

from datashape import discover, Categorical, DateTime
from datashape import string, object_, datetime_, Option
import datashape

import pandas as pd
import numpy as np

from ..convert import convert


possibly_missing = frozenset({string, datetime_})
try:
    from pandas.api.types import CategoricalDtype as categorical
except ImportError:
    categorical = type(pd.Categorical.dtype)
    assert categorical is not property


def dshape_from_pandas(col):
    if isinstance(col.dtype, categorical):
        return Categorical(col.cat.categories.tolist())
    elif col.dtype.kind == 'M':
        tz = getattr(col.dtype, 'tz', None)
        if tz is not None:
            # Pandas stores this as a pytz.tzinfo, but DataShape wants a
            # string.
            tz = str(tz)
        return Option(DateTime(tz=tz))

    dshape = datashape.CType.from_numpy_dtype(col.dtype)
    dshape = string if dshape == object_ else dshape
    return Option(dshape) if dshape in possibly_missing else dshape


@discover.register(pd.DataFrame)
def discover_dataframe(df):
    return len(df) * datashape.Record([(k, dshape_from_pandas(df[k]))
                                       for k in df.columns])


@discover.register((pd.Series, pd.Index))
def discover_1d(array_like):
    return len(array_like) * dshape_from_pandas(array_like)


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
    objects = df.select_dtypes(include=['object'])
    # NOTE: In pandas < 0.17, pd.to_datetime(' ') == datetime(...), which is
    # not what we want.  So we have to remove columns with empty or
    # whitespace-only strings to prevent erroneous datetime coercion.
    columns = [
        c for c in objects.columns
        if not np.any(objects[c].str.isspace() | objects[c].str.isalpha())
    ]
    df2 = objects[columns].apply(partial(pd.to_datetime, errors='ignore'))

    for c in df2.columns:
        df[c] = df2[c]
    return df


@convert.register(pd.Timestamp, datetime)
def convert_datetime_to_timestamp(dt, **kwargs):
    return pd.Timestamp(dt)


@convert.register((pd.Timestamp, pd.Timedelta), float)
def nan_to_nat(fl, **kwargs):
    try:
        if np.isnan(fl):
            # Only nan->nat edge
            return pd.NaT
    except TypeError:
        pass
    raise NotImplementedError()


@convert.register((pd.Timestamp, pd.Timedelta), (type(pd.NaT), type(None)))
def convert_null_or_nat_to_nat(n, **kwargs):
    return pd.NaT


@convert.register(pd.Timedelta, timedelta)
def convert_timedelta_to_pd_timedelta(dt, **kwargs):
    if dt is None:
        return pd.NaT
    return pd.Timedelta(dt)
