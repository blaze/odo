HDF5
====

The Hierarchical Data Format is a binary, self-describing format, supporting
regular strided and random access.  There are three main options in Python to
interact with HDF5

*  `h5py`_ - an unopinionated reflection of the HDF5 library
*  `pytables`_ - an opinionated version, adding extra features and conventions
*  `pandas.HDFStore`_ - a commonly used format among Pandas users.

All of these libraries create and read HDF5 files.  Unfortunately some of them
have special conventions that can only be understood by their library.  So a
given HDF5 file created some of these libraries may not be well understood by
the others.


Protocols
---------

If given an explicit object (not a string uri), like an ``h5py.Dataset``,
``pytables.Table`` or ``pandas.HDFStore`` then the ``odo`` project can
intelligently decide what to do.  If given a string, like
``myfile.hdf5::/data/path`` then ``odo`` defaults to using the vanilla
``h5py`` solution, the least opinionated of the three.

You can specify that you want a particular format with one of the following protocols

*  ``h5py://``
*  ``pytables://``
*  ``hdfstore://``


Limitations
-----------

Each library has limitations.

* H5Py does not like datetimes
* PyTables does not like variable length strings,
* Pandas does not like non-tabular data (like ``ndarrays``) and, if users
  don't select the ``format='table'`` keyword argument, creates HDF5 files
  that are not well understood by other libraries.

Our support for PyTables is admittedly weak.  We would love contributions here.


URIs
----

A URI to an HDF5 dataset includes a filename, and a datapath within that file.
Optionally it can include a protocol

Examples of HDF5 uris::

    myfile.hdf5::/data/path
    hdfstore://myfile.h5::/data/path


Conversions
-----------

The default paths in and out of HDF5 files include sequences of Pandas
``DataFrames`` and sequences of NumPy ``ndarrays``.::

    h5py.Dataset <-> chunks(np.ndarray)
    tables.Table <-> chunks(pd.DataFrame)
    pandas.AppendableFrameTable <-> chunks(pd.DataFrame)
    pandas.FrameFixed <-> DataFrame

.. _`h5py`: http://www.h5py.org/
.. _`PyTables`: http://pytables.github.io/index.html
.. _`pandas.HDFStore`: http://pandas.pydata.org/pandas-docs/stable/io.html#hdf5-pytables
