Adding a new Backend
====================

Q: *How do I add new nodes to the odo graph?*


Extend Functions
----------------

We extend Odo by implementing a few functions for each new type

*  ``discover`` - Return the DataShape_ of an object
*  ``convert`` - Convert data to new type
*  ``append`` - Append data on to existing data source
*  ``resource`` - Identify data by a string URI

We extend each of these by writing new small functions that we decorate with
types.  Odo will then pick these up, integrate them in to the network, and use
them when appropriate.

Discover
--------

Discover returns the DataShape_ of an object.  Datashape is a potentially
nested combination of shape and datatype.  It helps us to migrate metadata
consistently as we migrate the data itself.  This enables us to emerge with the
right dtypes even if we have to transform through potentially lossy formats.

Example
```````

.. code-block:: python

    >>> discover([1, 2, 3])
    dshape("3 * int32")

    >>> import numpy as np
    >>> x = np.empty(shape=(3, 5), dtype=[('name', 'O'), ('balance', 'f8')])
    >>> discover(x)
    dshape("3 * 5 * {name: string, balance: float64}")

Extend
``````

We import ``discover`` from the ``datashape`` library and extend it with a
type.

.. code-block:: python

   from datashape import discover, from_numpy

   @discover(pd.DataFrame)
   def discover_dataframe(df, **kwargs):
       shape = (len(df),)
       dtype = df.values.dtype
       return from_numpy(shape, dtype)

In this simple example we rely on convenience functions within datashape to
form a datashape from a numpy shape and dtype.  For more complex situations
(e.g. databases) it may be necessary to construct datashapes manually.


Convert
-------

Convert copies your data in to a new object with a different type.

Example
```````

.. code-block:: python

    >>> x = np.arange(5)
    >>> x
    array([0, 1, 2, 3, 4])

    >>> convert(list, x)
    [0, 1, 2, 3, 4]

    >>> import pandas as pd
    >>> convert(pd.Series, x)
    0    0
    1    1
    2    2
    3    3
    4    4
    dtype: int64

Extend
``````

Import convert from ``odo`` and register it with two types, one for the target
and one for the source

.. code-block:: python

   from odo import convert

   @convert.register(list, np.ndarray)
   def array_to_list(x, **kwargs):
       return x.tolist()

   @convert.register(pd.Series, np.ndarray)
   def array_to_series(x, **kwargs):
       return pd.Series(x)


Append
------

Append copies your data in to an existing dataset.

Example
```````

.. code-block:: python

    >>> x = np.arange(5)
    >>> x
    array([0, 1, 2, 3, 4])

    >>> L = [10, 20, 30]
    >>> _ = append(L, x)
    >>> L
    [10, 20, 30, 0, 1, 2, 3, 4]

Extend
``````

Import append from ``odo`` and register it with two types, one for the target
and one for the source.  Usually we teach ``odo`` how to append from one
preferred type and then use convert for all others

.. code-block:: python

   from odo import append

   @append.register(list, list)
   def append_list_to_list(tgt, src, **kwargs):
       tgt.extend(src)
       return tgt

   @append.register(list, object)  # anything else
   def append_anything_to_list(tgt, src, **kwargs):
       source_as_list = convert(list, src, **kwargs)
       return append(tgt, source_as_list, **kwargs)


Resource
--------

Resource creates objects from string URIs matched against regular expressions.

Example
```````

.. code-block:: python

   >>> resource('myfile.hdf5')
   <HDF5 file "myfile.hdf5" (mode r+)>

   >>> resource('myfile.hdf5::/data', dshape='10 * 10 * int32')
   <HDF5 dataset "data": shape (10, 10), type "<i4">

The objects it returns are ``h5py.File`` and ``h5py.Dataset`` respectively.  In
the second case resource found that the dataset did not exist so it created it.

Extend
``````

We import ``resource`` from ``odo`` and register it with regular expressions

.. code-block:: python

   from odo import resource

   import h5py

   @resource.register('.*\.hdf5')
   def resource(uri, **kwargs):
       return h5py.File(uri)


General Notes
-------------

We pass all keyword arguments from the top-level call to ``odo`` to *all*
functions.  This allows special keyword arguments to trickle down the right
right place, e.g. ``delimiter=';'`` makes it to the ``pd.read_csv`` call when
interacting with CSV files, but also means that all functions that you write
must expect and handle unwanted keyword arguments.  This often requires some
filtering on your part.

Even though all four of our abstract functions have a ``.register`` method they
operate in very different ways.  Convert is managed by networkx and path
finding, ``append`` and ``discover`` are managed by multipledispatch_, and
``resource`` is managed by regular expressions.

Examples are useful.  You may want to look at some of the ``odo`` source for
simple backends for help

    https://github.com/ContinuumIO/odo/tree/master/odo/backends

.. _DataShape : datashape.html
.. _multipledispatch: http://github.com/mrocklin/multipledispatch
