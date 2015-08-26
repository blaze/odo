SQL
===

Odo interacts with SQL databases through SQLAlchemy.  As a result, ``odo``
supports all databases that SQLAlchemy supports.  Through third-party
extensions, SQLAlchemy supports *most* databases.

.. warning::

   When putting an array like object such as a NumPy array or PyTables array
   into a database you *must* provide the column names in the form of a Record
   datashape. Without column names, it doesn't make sense to put an array into
   a database table, since a database table doesn't make sense without named
   columns. Remember, **there's no notion of dimensions indexed by integers**
   like there is with arrays, so the inability to put an array with unnamed
   columns into a database is intentional.

   Here's a failing example:

   .. code-block:: python

      >>> import numpy as np
      >>> from odo import odo
      >>> x = np.zeros((10, 2))
      >>> t = odo(x, 'sqlite:///db.db::x')  # this will NOT work

   Here's what to do instead:

   .. code-block:: python

      >>> t = odo(x, 'sqlite:///db.db::x',  # works because columns are named
      >>> ...     dshape='var * {a: float64, b: float64}')

URIs
----

Simple and complex examples of SQL uris::

    postgresql://localhost::accounts
    postgresql://username:password@54.252.14.53:10000/default::accounts

SQL uris consist of the following

* dialect protocol:  ``postgresql://``
* Optional authentication information:  ``username:password@``
* A hostname or network location with optional port:  ``54.252.14.53:10000``
* Optional database/schema name:  ``/default``
* A table name with the ``::`` separator:  ``::accounts``


Conversions
-----------

The default path in and out of a SQL database is to use the SQLAlchemy library
to consume iterators of Python dictionaries.  This method is robust but slow.::

    sqlalchemy.Table <-> Iterator
    sqlalchemy.Select <-> Iterator

For a growing subset of databases (``sqlite, MySQL, PostgreSQL, Hive,
RedShift``) we also use the CSV or JSON tools that come with those databases.
These are often an order of magnitude faster than the ``Python->SQLAlchemy``
route when they are available.::

    sqlalchemy.Table <- CSV
