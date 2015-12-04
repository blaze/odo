from __future__ import absolute_import, division, print_function

import pandas as pd

from ..convert import convert
from ..utils import ignoring


with ignoring(ImportError):
    from bokeh.models import ColumnDataSource


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
