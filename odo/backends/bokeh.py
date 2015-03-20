from __future__ import absolute_import, division, print_function

from ..convert import convert
import pandas as pd
try:
    from bokeh.models import ColumnDataSource
except ImportError:
    ColumnDataSource = type(None)

@convert.register(pd.DataFrame, ColumnDataSource)
def columndatasource_to_dataframe(cds, **kwargs):
    df = cds.to_df()
    return df[sorted(df.columns)]


@convert.register(ColumnDataSource, pd.DataFrame)
def dataframe_to_columndatasource(df, **kwargs):
    d = ColumnDataSource.from_df(df)
    if 'index' in d:
        d.pop('index')
    return ColumnDataSource(d)
