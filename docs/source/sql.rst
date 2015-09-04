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

For a growing subset of databases (sqlite, MySQL, PostgreSQL, Hive,
Redshift) we also use the CSV or JSON tools that come with those databases.
These are often an order of magnitude faster than the ``Python->SQLAlchemy``
route when they are available.::

    sqlalchemy.Table <- CSV


Primary and Foreign Key Relationships
-------------------------------------

.. versionadded:: 0.3.4

.. warning::

   Primary and foreign key relationship handling is an experimental feature and
   is subject to change.

Odo has experimental support for creating and discovering relational database
tables with primary keys and foreign key relationships.

Creating a new resource with a primary key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We create a new ``sqlalchemy.Table`` object with the resource function,
specifying the primary key in the ``dshape`` argument

   .. code-block:: python

      >>> from odo import resource
      >>> dshape = 'var * {id: !int64, name: string}'
      >>> products = resource('sqlite:///db.db::products', dshape=dshape)
      >>> products.c.id.primary_key
      True


Creating resources with foreign key relationships
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Creating a new resource with a foreign key relationship is only slightly more
complex.

As a motivating example, consider two tables ``products`` and ``orders``. The
``products`` table will be the table from the primary key example. The
``orders`` table will have a many-to-one relationship to the ``products``
table. We can create this like so

   .. code-block:: python

      >>> orders_dshape = """
      ... var * {
      ...    order_id: !int64,
      ...    product_id: map[int64, {id: !int64, name: string}]
      ... }
      ... """
      >>> orders = resource(
      ...     'sqlite:///db.db::orders',
      ...     dshape=orders_dshape,
      ...     foreign_keys={
      ...         'product_id': products.c.id,
      ...     }
      ... )
      >>> products.c.id in orders.c.product_id.foreign_keys
      True

There are two important things to note here.

   1. The general syntax for specifying the *type* of referring column is

      .. code-block:: python

         map[<referring column type>, <measure of the table being referred to>]

   2. Knowing the type isn't enough to specify a foreign key relationship. We
      also need to know the table that has the columns we want to refer to. The
      `foreign_keys` argument to the :func:`~odo.resource.resource` function
      fills this need. It accepts a dictionary mapping referring column
      names to referred to ``sqlalchemy.Column`` instances or strings such as
      ``products.id``.

There's also a shortcut syntax using type variables for specifying foreign
key relationships whose referred-to tables have very complex datashapes.

Instead of writing our ``orders`` table above as

   ::

      var * {order_id: !int64, product_id: map[int64, {id: !int64, name: string}]}

We can replace the value part of the ``map`` type with any word starting with a
capital letter. Often this is a single capital letter, such as ``T``

   ::

      var * {order_id: !int64, product_id: map[int64, T]}

Odo will automatically fill in the datashape for ``T`` by calling
:func:`~odo.discover` on the columns passed into the `foreign_keys` keyword
argument.

Finally, note that discovery of primary and foreign keys is done automatically,
if they already exist in the database so there's no need to specify
relationships or keys for tables that already have them.

Amazon Redshift
---------------

When using Amazon Redshift the error reporting leaves much to be desired.
Many errors look like this::

    InternalError: (psycopg2.InternalError) Load into table 'tmp0' failed.  Check 'stl_load_errors' system table for details.

If you're reading in CSV data from S3, check to make sure that

   1. The delimiter is correct. We can't correctly infer everything, so you may
      have to pass that value in as e.g., ``delimiter='|'``.
   2. You passed in the ``compression='gzip'`` keyword argument if your data
      are compressed as gzip files.

If you're still getting an error and you're sure both of the above are
correct, please report a bug on
`the odo issue tracker <https://github.com/blaze/odo/issues>`_

We have an open issue (:issue:`298`) to discuss how to better handle the
problem of error reporting when using Redshift.
