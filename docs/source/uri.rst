===========
URI strings
===========

Odo uses strings refer to data outside of Python.

Some example uris include the following::

    myfile.json
    myfiles.*.csv'
    postgresql://hostname::tablename
    mongodb://hostname/db::collection
    ssh://user@host:/path/to/myfile.csv
    hdfs://user@host:/path/to/myfile.csv


What sorts of URI's does ``odo`` support?
-----------------------------------------

* Paths to files on disk
    * ``.csv``
    * ``.json``
    * ``.txt/log``
    * ``.csv.gz/json.gz``
    * ``.hdf5``
    * ``.hdf5::/datapath``
    * ``.bcolz``
    * ``.xls(x)``
    * ``.sas7bdat``
* Collections of files on disk
    * ``*.csv``
* SQLAlchemy strings
    * ``sqlite:////absolute/path/to/myfile.db::tablename``
    * ``sqlite:////absolute/path/to/myfile.db``  (specify a particular table)
    * ``postgresql://username:password@hostname:port``
    * ``impala://hostname`` (uses ``impyla``)
    * *anything supported by SQLAlchemy*
* MongoDB Connection strings
    * ``mongodb://username:password@hostname:port/database_name::collection_name``
* Remote locations via SSH, HDFS and Amazon's S3
    * ``ssh://user@hostname:/path/to/data``
    * ``hdfs://user@hostname:/path/to/data``
    * ``s3://path/to/data``


Separating parts with ``::``
----------------------------

Many forms of data have two paths, the path to the file and then the path
within the file.  For example we refer to the table ``accounts`` in a Postgres database like so::

    postgresql://localhost::accounts

In this case the separator ``::`` separtates the database
``postgreqsl://localhost`` from the table within the database, ``accounts``.

This also occurs in HDF5 files which have an internal datapath::

    myfile.hdf5::/path/to/data


Specifying protocols with ``://``
---------------------------------

The database string `sqlite:///data/my.db` is specific to SQLAlchemy, but
follows a common format, notably::

    Protocol:  sqlite://
    Filename:  data/my.db

Odo also uses protocols in many cases to give extra hints on how to
handle your data.  For example Python has a few different libraries to
handle HDF5 files (``h5py``, ``pytables``, ``pandas.HDFStore``).  By default
when we see a URI like ``myfile.hdf5`` we currently use ``h5py``.  To
override this behavior you can specify a protocol string like::

    hdfstore://myfile.hdf5

to specify that you want to use the special ``pandas.HDFStore`` format.

*Note:* sqlite strings are a little odd in that they use three
slashes by default (e.g. ``sqlite:///my.db``) and *four* slashes when
using absolute paths (e.g. ``sqlite:////Users/Alice/data/my.db``).


How it works
------------

We match URIs by to a collection of regular expressions.  This is handled by
the ``resource`` function.

.. code-block:: python

   >>> from odo import resource
   >>> resource('sqlite:///data.db::iris')
   Table('iris', MetaData(bind=Engine(sqlite:///myfile.db)), ...)

When we use a string in ``odo`` this is actually just shorthand for calling
``resource``.

.. code-block:: python

   >>> from odo import odo
   >>> odo('some-uri', list)            # When you write this
   >>> odo(resource('some-uri'), list)  # actually this happens

Notably, URIs are just syntactic sugar, you don't have to use them.  You can
always construct the object explicitly.  Odo invents very few types,
preferring instead to use standard projects within the Python ecosystem like
``sqlalchemy.Table`` or ``pymongo.Collection``.  If your applicaiton also uses
these types then it's likely that ``odo`` already works with your data.


Can I extend this to my own types?
----------------------------------

Absolutely.  Lets make a little resource function to load pickle files.

.. code-block:: python

   import pickle
   from odo import resource

   @resource.register('.*\.pkl')  # match anything ending in .pkl
   def resource_pickle(uri, **kwargs):
       with open(uri) as f:
           result = pickle.load(f)
       return result

You can implement this kind of function for your own data type.  Here we just
loaded whatever the object was into memory and returned it, a rather simplistic
solution.  Usually we return an object with a particular type that represents
that data well.
