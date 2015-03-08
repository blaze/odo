from __future__ import absolute_import, division, print_function

from .regex import RegexDispatcher


__all__ = 'resource'


resource = RegexDispatcher('resource')


@resource.register('.*', priority=1)
def resource_all(uri, *args, **kwargs):
    """ Refer to data by strings

    Translate a string pointing to data into a Python object pointing to that
    data.

    Filenames for common formats are valid URIs

    >>> resource('myfile.csv')  # doctest: +SKIP
    <odo.CSV ...>

    Database connection strings may embed connection information

    >>> resource('postgresql://user:pass@hostname/db::tablename')  # doctest: +SKIP
    Table('tablename', MetaData(bind=Engine(postgres://...  ... )))

    When possible ``resource`` returns an object from another popular library.
    In the case above ``resource`` gives you a ``sqlalchemy.Table`` object.

    What kinds of strings does resource support?
    --------------------------------------------

    Filenames with common formats as well as collections of those files

        myfile.csv - CSV files
        myfile.txt - Text files
        myfile.json - JSON and line-delimited JSON
        myfile.*.csv - Collections of files

    Some files, like HDF5 files or sqlite files, require a second piece of
    information, like a datapath or tablename.  We use the separator ``::`` in
    these cases.

        myfile.hdf5::/data/path
        sqlite://myfile.db::tablename

    Many systems use protocols like ``sqlite://`` to specify additional
    information.  We also use these to disambiguate when one file format, like
    HDF5, might have several possible internal formats, like standard HDF5 or
    Pandas' HDFStore

        myfile.hdf5::/data/path
        hdfstore://myfile.hdf5::/data/path

    or JSON vs JSON-lines

        json://myfile.json
        jsonlines://myfile.json

    These strings are defined as regular expressions.  See ``resource.funcs``
    to see what your installation currently supports.

    >>> resource.funcs  # doctest: +SKIP
    {'.+\.csv)(\.gz|\.bz)?': <function odo.backends.csv.resource_csv>,
     '.+\.json)(\.gz|\.bz)?': <function odo.backends.json.resource_json>,
     '\w+sql\s+://.+': <function odo.backends.sql.resource_sql>,
     ...}

    Relation with ``odo``
    ----------------------

    The main ``odo`` function uses ``resource`` to resolve string URIs.

    The following call:

    >>> odo('some-sorce', target)  # doctest: +SKIP

    is shorthand for the following:

    >>> odo(resource('some-sorce'), target)  # doctest: +SKIP

    Create datasets with resource
    -----------------------------

    Resource can also create new datasets by provding a datashape

    >>> resource('myfile.hdf5::/data', dshape='1000 * 1000 * float32')  # doctest: +SKIP
    <HDF5 dataset "data": shape (1000, 1000), type "<f4">

    To learn more about datashapes see the function ``discover``

    See Also
    --------

    odo
    discover
    """
    raise NotImplementedError("Unable to parse uri to data resource: " + uri)


@resource.register('.+::.+', priority=15)
def resource_split(uri, *args, **kwargs):
    uri, other = uri.rsplit('::', 1)
    return resource(uri, other, *args, **kwargs)
