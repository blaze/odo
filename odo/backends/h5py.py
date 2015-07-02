from __future__ import absolute_import, division, print_function

import h5py
import os

import datashape
from datashape import DataShape, Record, to_numpy, discover
from datashape.predicates import isrecord
from datashape.dispatch import dispatch
import numpy as np
from toolz import keyfilter

from ..numpy_dtype import dshape_to_numpy
from ..append import append
from ..convert import convert, ooc_types
from ..create import create
from ..resource import resource
from ..chunks import chunks


h5py_attributes = ['chunks', 'compression', 'compression_opts', 'dtype',
                   'fillvalue', 'fletcher32', 'maxshape', 'shape']


try:
    unicode_dtype = h5py.special_dtype(vlen=unicode)
except NameError:
    unicode_dtype = h5py.special_dtype(vlen=str)


@discover.register((h5py.Group, h5py.File))
def discover_h5py_group_file(g):
    return DataShape(Record([[k, discover(v)] for k, v in g.items()]))


def record_dshape_replace(dshape, old, new):
    """Recursively replace all instances of `old` with `new` in the record
    dshape `dshape`.

    Examples
    --------
    >>> from datashape import Record, string, object_, dshape
    >>> ds = DataShape(Record([('a', 'int64'),
    ...                        ('b', 10 * Record([('c', 'object')])),
    ...                        ('d', 'int64')]))
    ...
    >>> Record(list(record_dshape_replace(ds, object_, string)))
    dshape("{a: int64, b: 10 * {c: object}, d: int64}")
    """
    assert isrecord(dshape), 'input dshape must be a record'

    for name, subshape in dshape.measure.fields:
        if subshape == old:
            yield name, new
        else:
            if isrecord(subshape):
                yield record_dshape_replace(subshape, old, new)
            else:
                yield name, subshape


@discover.register(h5py.Dataset)
def discover_h5py_dataset(d):
    dshape = datashape.from_numpy(d.shape, d.dtype)
    shape, measure = dshape.shape, dshape.measure
    if not isrecord(measure):
        if dshape == datashape.object_:
            args = shape + (datashape.string,)
            return DataShape(*args)
        return dshape
    else:
        records = list(record_dshape_replace(measure, datashape.object_,
                                             datashape.string))
        args = shape + (datashape.Record(records),)
        return DataShape(*args)


def dtype_replace(dtype, old, new):
    """Replace the subdtype `old` in `subdtype` with `new`.

    Parameters
    ----------
    dtype, old, new : dtype

    Examples
    --------
    >>> dt = np.dtype([('a', 'int64'), ('b', 'object'),
    ...                ('c', [('d', 'object'), ('e', 'float64')])])
    ...
    >>> r = np.dtype(list(dtype_replace(dt, 'int64', 'float64')))
    >>> r
    dtype([('a', '<f8'), ('b', 'O'), ('c', [('d', 'O'), ('e', '<f8')])])
    """
    names = dtype.names

    assert names is not None, 'dtype must be record-like'

    for name, subdtype in zip(names, map(dtype.__getitem__, names)):
        if subdtype == old:
            yield name, new
        else:
            if subdtype.names is not None:
                yield name, list(dtype_replace(subdtype, old, new))
            else:
                yield name, subdtype


def varlen_dtype(dt):
    """Inject variable length string element for object dtype

    Examples
    --------
    >>> dt = np.dtype('object')
    >>> dt
    dtype('O')
    >>> r = varlen_dtype(dt)
    >>> r
    dtype('O')
    >>> r.metadata['vlen']  # doctest: +SKIP
    <type 'unicode'>
    >>> dt = np.dtype([('a', 'int64'), ('b', 'object'),
    ...                ('c', [('d', 'object'), ('e', 'float64')])])
    ...
    >>> dt['b'].metadata
    >>> r = varlen_dtype(dt)
    >>> r
    dtype([('a', '<i8'), ('b', 'O'), ('c', [('d', 'O'), ('e', '<f8')])])
    >>> r['b'].metadata['vlen']  # doctest: +SKIP
    <type 'unicode'>
    """
    if dt == np.object_:
        return unicode_dtype
    elif dt.names is None:  # some kind of non record like dtype
        return dt
    else:
        return np.dtype(list(dtype_replace(dt, np.dtype('object'),
                                           unicode_dtype)))


def dataset_from_dshape(file, datapath, ds, **kwargs):
    dtype = varlen_dtype(dshape_to_numpy(ds))

    if datashape.var not in list(ds):
        shape = tuple(map(int, ds.shape))
    elif datashape.var not in list(ds)[1:]:
        shape = (0,) + tuple(map(int, ds.shape[1:]))
    else:
        raise ValueError("Don't know how to handle varlen nd shapes")

    if shape:
        kwargs['chunks'] = kwargs.get('chunks', True)
        kwargs['maxshape'] = kwargs.get('maxshape', (None,) + shape[1:])

    kwargs2 = keyfilter(h5py_attributes.__contains__, kwargs)
    return file.require_dataset(datapath, shape=shape, dtype=dtype, **kwargs2)


def create_from_datashape(group, ds, name=None, **kwargs):
    if not isrecord(ds):
        raise ValueError(
            "Trying to create an HDF5 file with non-record datashape failed\n"
            "Perhaps you forgot to specify a datapath?\n"
            "\tdshape: %s\n"
            "If you're using odo consider the following change\n"
            "\tBefore: odo(data, 'myfile.hdf5')\n"
            "\tAfter:  odo(data, 'myfile.hdf5::/datapath')" % ds)
    if isinstance(ds, DataShape) and len(ds) == 1:
        ds = ds[0]
    for name, sub_ds in ds.dict.items():
        if isrecord(sub_ds):
            g = group.require_group(name)
            create_from_datashape(g, sub_ds, **kwargs)
        else:
            dataset_from_dshape(file=group.file,
                                datapath='/'.join([group.name, name]),
                                ds=sub_ds, **kwargs)


@create.register(h5py.File)
def create_h5py_file(cls, path=None, dshape=None, **kwargs):
    f = h5py.File(path)
    create_from_datashape(f, dshape, **kwargs)
    return f


@append.register(h5py.Dataset, np.ndarray)
def append_h5py(dset, x, **kwargs):
    if not sum(x.shape):
        return dset
    shape = list(dset.shape)
    shape[0] += len(x)
    dset.resize(shape)
    dset[-len(x):] = x
    return dset


@append.register(h5py.Dataset, chunks(np.ndarray))
def append_h5py(dset, c, **kwargs):
    for chunk in c:
        append(dset, chunk)
    return dset


@append.register(h5py.Dataset, object)
def append_h5py(dset, x, **kwargs):
    return append(dset, convert(chunks(np.ndarray), x, **kwargs), **kwargs)


@convert.register(np.ndarray, h5py.Dataset, cost=3.0)
def h5py_to_numpy(dset, force=False, **kwargs):
    if dset.size > 1e9:
        raise MemoryError(("File size is large: %0.2f GB.\n"
                           "Convert with flag force=True to force loading") %
                          (dset.size / 1e9))
    else:
        return dset[:]


@convert.register(chunks(np.ndarray), h5py.Dataset, cost=3.0)
def h5py_to_numpy_chunks(dset, chunksize=2 ** 20, **kwargs):
    def load():
        for i in range(0, dset.shape[0], chunksize):
            yield dset[i: i + chunksize]
    return chunks(np.ndarray)(load)


@resource.register('h5py://.+', priority=11)
def resource_h5py(uri, datapath=None, dshape=None, expected_dshape=None,
                  **kwargs):
    if uri.startswith('h5py://'):
        uri = uri[len('h5py://'):]
    f = h5py.File(uri)
    olddatapath = datapath
    if datapath is not None and datapath in f:
        old_dset = f[datapath]
        if expected_dshape is not None:
            dshape = expected_dshape
            assert dshape == discover(old_dset)
    if dshape is not None:
        ds = datashape.dshape(dshape)
        if datapath:
            while ds and datapath:
                datapath, name = datapath.rsplit('/', 1)
                ds = Record([[name, ds]])
            ds = datashape.dshape(ds)
        f.close()
        f = create(h5py.File, path=uri, dshape=ds, **kwargs)
    if olddatapath:
        return f[olddatapath]
    else:
        return f


@resource.register(r'^(?!hdfstore).+\.(hdf5|h5)', priority=10)
def resource_hdf5(uri, *args, **kwargs):
    return resource_h5py(uri, *args, **kwargs)


@dispatch((h5py.Group, h5py.Dataset))
def drop(h):
    del h.file[h.name]


@dispatch(h5py.File)
def drop(h):
    fn = h.filename
    h.close()
    os.remove(fn)


ooc_types.add(h5py.Dataset)
