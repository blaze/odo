from __future__ import absolute_import

from castra import Castra
from datashape import from_numpy, var
from datashape.dispatch import dispatch
import pandas as pd

from ..append import append
from ..compatibility import items
from ..convert import convert
from ..chunks import chunks
from ..drop import drop
from ..resource import resource
from ..utils import ignoring


@dispatch(Castra)
def discover(c):
    return var * from_numpy((), list(items(c.dtypes)))


@append.register(Castra, pd.DataFrame)
def append_df_to_castra(c, df, **kwargs):
    return c.extend(df)


@append.register(Castra, chunks(pd.DataFrame), )
def append_dfs_to_castra(c, dfs, freq=None, **kwargs):
    c.extend_sequence(dfs, freq=freq)


@convert.register(Castra, pd.DataFrame)
def df_to_castra(df,
                 path=None,
                 categories=None,
                 readonly=False,
                 **kwargs):
    c = Castra(
        path=path,
        template=df,
        categories=categories,
        readonly=readonly,
    )
    c.extend(df)
    return c


@convert.register(pd.DataFrame, Castra)
def castra_to_df(c, **kwargs):
    return c[:]


@drop.register(Castra)
def drop(c):
    c.drop()


with ignoring(ImportError):
    import dask.dataframe as dd

    @convert.register(dd.DataFrame, Castra)
    def castra_to_dask_df(c, **kwargs):
        return c.to_dask()

    @convert.register(Castra, dd.DataFrame)
    def dask_df_to_castra(df,
                          fn=None,
                          categories=None,
                          sorted_index_column=None,
                          compute=True,
                          **kwargs):
        return df.to_castra(
            fn=fn,
            categories=categories,
            sorted_index_column=sorted_index_column,
            compute=compute,
        )


@resource.register(r'.+\.castra')
def castra_resource(uri, **kwargs):
    return Castra(uri)
