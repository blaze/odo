HDF5
====

The Hierarchical Data Format is a binary, self-describing format, supporting
regular strided and random access.  There are three main options in Python to
interact with HDF5

*  ``h5py`` - an unopinionated reflection of the HDF5 library
*  ``pytables`` - an opinionated version, adding extra features and conventions
*  ``pandas HDFStore`` - a commonly used format among Pandas users.

All of these libraries create and read HDF5 files.  Unfortunately some of them
have special conventions that can only be understood by their library.  For
example, pandas ``HDFStore`` format is specific to Pandas and is not widely
understood by other tools.

Protocols
---------

If given an explicitly object, like an ``h5py.Dataset``, ``pytables.Table`` or
``pandas.HDFStore`` then the ``into`` project can intelligently decide what to
do.  If given a string, like ``myfile.hdf5::/data/path`` then ``into`` defaults
to using the vanilla ``h5py`` solution.

You can specify that you want a particular format with one of the following protocols

*  ``h5py://``
*  ``pytables://``
*  ``hdfstore://``

Limitations
-----------

H5Py does not like datetimes, PyTables does not like variable length strings,
Pandas does not like non-tabular data (like ``ndarrays``).

Our support for PyTables is admittedly weak.  We would love contributions here.


URIs
----

A URI to an HDF5 dataset includes a filename, and a datapath within that file.
Optionally it can include a protocol

Examples of CSV uris::

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
