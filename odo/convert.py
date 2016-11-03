from __future__ import absolute_import, division, print_function

import numpy as np
import pandas as pd
from datashape.predicates import isscalar
from toolz import concat, partition_all, compose
from collections import Iterator, Iterable
import datashape
from datashape import discover
from .core import NetworkDispatcher, ooc_types
from .chunks import chunks, Chunks
from .numpy_dtype import dshape_to_numpy
from .utils import records_to_tuples
from functools import partial


convert = NetworkDispatcher('convert')


@convert.register(np.ndarray, pd.DataFrame, cost=0.2)
def dataframe_to_numpy(df, dshape=None, **kwargs):
    dtype = dshape_to_numpy(dshape or discover(df))
    x = df.to_records(index=False)
    if x.dtype != dtype:
        x = x.astype(dtype)
    return x


@convert.register(pd.DataFrame, np.ndarray, cost=1.0)
def numpy_to_dataframe(x, dshape, **kwargs):
    dtype = x.dtype
    names = dtype.names
    if names is None:
        if dtype.kind == 'm':
            # pandas does not do this conversion for us but doesn't work
            # with non 'ns' unit timedeltas
            x = x.astype('m8[ns]')
    else:
        fields = dtype.fields
        new_dtype = []
        should_astype = False
        for name in names:
            original_field_value = fields[name][0]
            if original_field_value.kind == 'm':
                # pandas does not do this conversion for us but doesn't work
                # with non 'ns' unit timedeltas
                new_dtype.append((name, 'm8[ns]'))

                # perform the astype at the end of the loop
                should_astype = True
            else:
                new_dtype.append((name, original_field_value))

        if should_astype:
            x = x.astype(new_dtype)

    df = pd.DataFrame(x, columns=getattr(dshape.measure, 'names', names))
    return df


@convert.register(pd.Series, np.ndarray, cost=1.0)
def numpy_to_series(x, **kwargs):
    names = x.dtype.names
    if names is not None:
        if len(names) > 1:
            raise ValueError('passed in an ndarray with more than 1 column')
        name, = names
        return pd.Series(x[name], name=name)
    return pd.Series(x)


@convert.register(pd.Series, pd.DataFrame, cost=0.1)
def DataFrame_to_Series(x, **kwargs):
    assert len(x.columns) == 1
    return x[x.columns[0]]


@convert.register(pd.DataFrame, pd.Series, cost=0.1)
def series_to_dataframe(x, **kwargs):
    return x.to_frame()


@convert.register(np.recarray, np.ndarray, cost=0.0)
def ndarray_to_recarray(x, **kwargs):
    return x.view(np.recarray)

@convert.register(np.ndarray, np.recarray, cost=0.0)
def recarray_to_ndarray(x, **kwargs):
    return x.view(np.ndarray)


higher_precision_freqs = frozenset(('ns', 'ps', 'fs', 'as'))


@convert.register(np.ndarray, pd.Series, cost=0.1)
def series_to_array(s, dshape=None, **kwargs):
    # if we come from a node that can't be discovered we need to discover
    # on s
    dtype = dshape_to_numpy(datashape.dshape(dshape or discover(s)))
    sdtype = s.dtype
    values = s.values

    # don't lose precision of datetime64 more precise than microseconds
    if ((issubclass(sdtype.type, np.datetime64) and
            np.datetime_data(sdtype)[0] in higher_precision_freqs) or
            s.dtype == dtype):
        return values
    try:
        return values.astype(dtype)
    except ValueError:  # object series and record dshape, e.g., a frame row
        return values


@convert.register(list, np.ndarray, cost=10.0)
def numpy_to_list(x, **kwargs):
    dt = None
    if x.dtype == 'M8[ns]':
        dt = 'M8[us]'  # lose precision when going to Python datetime
    if x.dtype.fields and any(x.dtype[n] == 'M8[ns]' for n in x.dtype.names):
        dt = [(n, 'M8[us]' if x.dtype[n] == 'M8[ns]' else x.dtype[n])
              for n in x.dtype.names]
    if dt:
        return x.astype(dt).tolist()
    else:
        return x.tolist()


@convert.register(np.ndarray, chunks(np.ndarray), cost=1.0)
def numpy_chunks_to_numpy(c, **kwargs):
    return np.concatenate(list(c))


@convert.register(chunks(np.ndarray), np.ndarray, cost=0.5)
def numpy_to_chunks_numpy(x, chunksize=2**20, **kwargs):
    return chunks(np.ndarray)(
            lambda: (x[i:i+chunksize] for i in range(0, x.shape[0], chunksize)))


@convert.register(pd.DataFrame, chunks(pd.DataFrame), cost=1.0)
def chunks_dataframe_to_dataframe(c, **kwargs):
    c = list(c)
    if not c:  # empty case
        return pd.DataFrame(columns=kwargs.get('dshape').measure.names)
    else:
        return pd.concat(c, axis=0, ignore_index=True)


@convert.register(chunks(pd.DataFrame), pd.DataFrame, cost=0.5)
def dataframe_to_chunks_dataframe(x, chunksize=2**20, **kwargs):
    return chunks(pd.DataFrame)(
            lambda: (x.iloc[i:i+chunksize] for i in range(0, x.shape[0], chunksize)))

def ishashable(x):
    try:
        hash(x)
        return True
    except:
        return False


@convert.register(set, (list, tuple), cost=5.0)
def iterable_to_set(x, **kwargs):
    if x and isinstance(x[0], (tuple, list)) and not ishashable(x):
        x = map(tuple, x)
    return set(x)


@convert.register(list, (tuple, set), cost=1.0)
def iterable_to_list(x, **kwargs):
    return list(x)


@convert.register(tuple, (list, set), cost=1.0)
def iterable_to_tuple(x, **kwargs):
    return tuple(x)


def element_of(seq):
    """

    >>> element_of([1, 2, 3])
    1
    >>> element_of([[1, 2], [3, 4]])
    1
    """
    while isinstance(seq, list) and seq:
        seq = seq[0]
    return seq


@convert.register(np.ndarray, list, cost=10.0)
def list_to_numpy(seq, dshape=None, **kwargs):
    if isinstance(element_of(seq), dict):
        seq = list(records_to_tuples(dshape, seq))
    if (seq and isinstance(seq[0], Iterable) and not ishashable(seq[0]) and
            not isscalar(dshape)):
        seq = list(map(tuple, seq))
    return np.array(seq, dtype=dshape_to_numpy(dshape))


@convert.register(Iterator, list, cost=0.001)
def list_to_iterator(L, **kwargs):
    return iter(L)


@convert.register(list, Iterator, cost=1.0)
def iterator_to_list(seq, **kwargs):
    return list(seq)


@convert.register(Iterator, (chunks(pd.DataFrame), chunks(np.ndarray)), cost=10.0)
def numpy_chunks_to_iterator(c, **kwargs):
    return concat(convert(Iterator, chunk, **kwargs) for chunk in c)


@convert.register(chunks(np.ndarray), Iterator, cost=10.0)
def iterator_to_numpy_chunks(seq, chunksize=1024, **kwargs):
    seq2 = partition_all(chunksize, seq)
    try:
        first, rest = next(seq2), seq2
    except StopIteration:  # seq is empty
        def _():
            yield convert(np.ndarray, [], **kwargs)
    else:
        x = convert(np.ndarray, first, **kwargs)

        def _():
            yield x
            for i in rest:
                yield convert(np.ndarray, i, **kwargs)
    return chunks(np.ndarray)(_)


@convert.register(chunks(pd.DataFrame), Iterator, cost=10.0)
def iterator_to_DataFrame_chunks(seq, chunksize=1024, **kwargs):
    seq2 = partition_all(chunksize, seq)

    add_index = kwargs.get('add_index', False)
    if not add_index:
        # Simple, we can dispatch to dask...
        f = lambda d: convert(pd.DataFrame, d, **kwargs)
        data = [partial(f, d) for d in seq2]
        if not data:
            data = [convert(pd.DataFrame, [], **kwargs)]
        return chunks(pd.DataFrame)(data)

    # TODO: Decide whether we should support the `add_index` flag at all.
    # If so, we need to post-process the converted DataFrame objects sequencially,
    # so we can't parallelize the process.
    try:
        first, rest = next(seq2), seq2
    except StopIteration:
        def _():
            yield convert(pd.DataFrame, [], **kwargs)
    else:
        df = convert(pd.DataFrame, first, **kwargs)
        df1, n1 = _add_index(df, 0)

        def _():
            n = n1
            yield df1
            for i in rest:
                df = convert(pd.DataFrame, i, **kwargs)
                df, n = _add_index(df, n)
                yield df
    return chunks(pd.DataFrame)(_)


def _add_index(df, start, _idx_type=getattr(pd, 'RangeIndex',
                                            compose(pd.Index, np.arange))):
    stop = start + len(df)
    idx = _idx_type(start=start, stop=stop)
    df.index = idx
    return df, stop


@convert.register(tuple, np.record)
def numpy_record_to_tuple(rec, **kwargs):
    return rec.tolist()


@convert.register(chunks(np.ndarray), chunks(pd.DataFrame), cost=0.5)
def chunked_pandas_to_chunked_numpy(c, **kwargs):
    return chunks(np.ndarray)(lambda: (convert(np.ndarray, chunk, **kwargs) for chunk in c))

@convert.register(chunks(pd.DataFrame), chunks(np.ndarray), cost=0.5)
def chunked_numpy_to_chunked_pandas(c, **kwargs):
    return chunks(pd.DataFrame)(lambda: (convert(pd.DataFrame, chunk, **kwargs) for chunk in c))


@convert.register(chunks(np.ndarray), chunks(list), cost=10.0)
def chunked_list_to_chunked_numpy(c, **kwargs):
    return chunks(np.ndarray)(lambda: (convert(np.ndarray, chunk, **kwargs) for chunk in c))

@convert.register(chunks(list), chunks(np.ndarray), cost=10.0)
def chunked_numpy_to_chunked_list(c, **kwargs):
    return chunks(list)(lambda: (convert(list, chunk, **kwargs) for chunk in c))

@convert.register(chunks(Iterator), chunks(list), cost=0.1)
def chunked_list_to_chunked_iterator(c, **kwargs):
    return chunks(Iterator)(c.data)

@convert.register(chunks(list), chunks(Iterator), cost=0.1)
def chunked_Iterator_to_chunked_list(c, **kwargs):
    return chunks(Iterator)(lambda: (convert(Iterator, chunk, **kwargs) for chunk in c))

@convert.register(Iterator, chunks(Iterator), cost=0.1)
def chunked_iterator_to_iterator(c, **kwargs):
    return concat(c)


ooc_types |= set([Iterator, Chunks])
