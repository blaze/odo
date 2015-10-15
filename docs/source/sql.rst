SQL
===

Odo interacts with SQL databases through SQLAlchemy.  As a result, ``odo``
supports all databases that SQLAlchemy supports.  Through third-party
extensions, SQLAlchemy supports *most* databases.

.. warning::

   When putting an array-like object such as a NumPy array into a database you
   *must* provide the column names in the form of a Record datashape.

   .. note::

      Without column names, it doesn't make sense to put an array into a
      database table, since a database table doesn't make sense without named
      columns. The inability to put an array with unnamed columns into a
      database is intentional.

   Here's a failing example:

   .. code-block:: python

      >>> import numpy as np
      >>> from odo import odo
      >>> x = np.zeros((10, 2))
      >>> t = odo(x, 'sqlite:///db.db::x')  # this will NOT work
      Traceback (most recent call last):
          ...
      TypeError: dshape measure must be a record type e.g., "{a: int64, b: int64}". Input measure is ctype("float64")

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


Executing Odo Against Databases
-------------------------------

Sqlalchemy allows objects to be bound to a particular database connection. This
is known as the 'bind' of the object, or that the object is 'bound'.

By default, odo expects to be working with either bound sqlalchemy objects or
uris to tables.

For example, when working with a sqlalchemy object, one must be sure to pass a
bound metadata to the construction of your tables.

.. code-block:: python

   >>> import sqlalchemy as sa
   >>> sa.MetaData()
   >>> tbl = sa.Table(
   ...     'tbl',
   ...     metadata,
   ...     sa.Column('a', sa.Integer, primary_key=True),
   ... )
   >>> odo([[1], [2], [3]], tbl, dshape='var * {a: int}')  # this will NOT work
   Traceback (most recent call last):
        ...
   UnboundExecutionError: Table object 'tbl' is not bound to an Engine or Connection.  Execution can not proceed without a database to execute against.

We have two options for binding metadata to objects, we can explicitly bind our
tables, or we can pass it to odo as a keyword argument.

Here is an example of constructing the table with a bound metadata:

.. code-block:: python

   >>> import sqlalchemy as sa
   >>> metadata = sa.MetaData(bind='sqlite:///db.db')  # NOTE: pass the uri to the db here
   >>> tbl = sa.Table(
   ...     'tbl',
   ...     metadata,
   ...     sa.Column('a', sa.Integer, primary_key=True),
   ... )
   >>> odo([[1], [2], [3]], tbl)  # this know knows where to field the table.

Here is an example of passing the bind to odo:

.. code-block:: python

   >>> import sqlalchemy as sa
   >>> sa.MetaData()
   >>> tbl = sa.Table(
   ...     'tbl',
   ...     metadata,
   ...     sa.Column('a', sa.Integer, primary_key=True),
   ... )
   >>> bind = 'sqlite:///db.db'
   >>> odo([[1], [2], [3]], tbl, dshape='var * {a: int}', bind=bind)  # pass the bind to odo here

Here, the bind may be either a uri to a database, or a sqlalchemy Engine object.

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
specifying the primary key in the ``primary_key`` argument

   .. code-block:: python

      >>> from odo import resource
      >>> dshape = 'var * {id: int64, name: string}'
      >>> products = resource(
      ...     'sqlite:///db.db::products',
      ...     dshape=dshape,
      ...     primary_key=['id'],
      ... )
      >>> products.c.id.primary_key
      True

Compound primary keys are created by passing the list of columns that form the
primary key. For example

   .. code-block:: python

      >>> dshape = """
      ... var * {
      ...     product_no: int32,
      ...     product_sku: string,
      ...     name: ?string,
      ...     price: ?float64
      ... }
      ... """
      >>> products = resource(
      ...     'sqlite:///%s::products' % fn,
      ...     dshape=dshape,
      ...     primary_key=['product_no', 'product_sku']
      ... )

Here, the column pair ``product_no, product_sku`` make up the compound primary
key of the ``products`` table.

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
      ...    order_id: int64,
      ...    product_id: map[int64, {id: int64, name: string}]
      ... }
      ... """
      >>> orders = resource(
      ...     'sqlite:///db.db::orders',
      ...     dshape=orders_dshape,
      ...     primary_key=['order_id'],
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

      var * {order_id: int64, product_id: map[int64, {id: int64, name: string}]}

We can replace the value part of the ``map`` type with any word starting with a
capital letter. Often this is a single capital letter, such as ``T``

   ::

      var * {order_id: int64, product_id: map[int64, T]}

Odo will automatically fill in the datashape for ``T`` by calling
:func:`~odo.discover` on the columns passed into the `foreign_keys` keyword
argument.

Finally, note that discovery of primary and foreign keys is done automatically
if the relationships already exist in the database so it isn't necessary to
specify them if they've already been created elsewhere.

More Complex Foreign Key Relationships
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Odo supports creation and discovery of self referential foreign key
relationships as well as foreign keys that are elements of a compound primary
key. The latter are usually seen when creating a many-to-many relationship via
a `junction table <https://en.wikipedia.org/wiki/Junction_table>`_.

Self referential relationships are most easily specified using type variables
(see the previous section for a description of how that works). Using the
example of a management hierarchy:

   .. code-block:: python

      >>> dshape = 'var * {eid: int64, name: ?string, mgr_eid: map[int64, T]}'
      >>> t = resource(
      ...     'sqlite:///%s::employees' % fn,
      ...     dshape=dshape,
      ...     primary_key=['eid'],
      ...     foreign_keys={'mgr_eid': 'employees.eid'}
      ... )

   .. note::

      Currently odo only recurses one level before terminating as we don't yet
      have a syntax for truly expressing recursive types in datashape

Here's an example of creating a junction table (whose foreign keys form a
compound primary key) using a modified version of the traditional
`suppliers and parts database <https://en.wikipedia.org/wiki/Suppliers_and_Parts_database>`_:

   .. code-block:: python

      >>> suppliers = resource(
      ...     'sqlite:///%s::suppliers' % fn,
      ...     dshape='var * {id: int64, name: string}',
      ...     primary_key=['id']
      ... )
      >>> parts = resource(
      ...     'sqlite:///%s::parts' % fn,
      ...     dshape='var * {id: int64, name: string, region: string}',
      ...     primary_key=['id']
      ... )
      >>> suppart = resource(
      ...     'sqlite:///%s::suppart' % fn,
      ...     dshape='var * {supp_id: map[int64, T], part_id: map[int64, U]}',
      ...     primary_key=['supp_id', 'part_id'],
      ...     foreign_keys={
      ...         'supp_id': suppliers.c.id,
      ...         'part_id': parts.c.id
      ...     }
      ... )
      >>> from odo import discover
      >>> print(discover(suppart))
      var * {
          supp_id: map[int64, {id: int64, name: string}],
          part_id: map[int64, {id: int64, name: string, region: string}]
      }

Foreign Key Relationship Failure Modes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some databases support the notion of having a foreign key reference one column
from another table's compound primary key. For example

   .. code-block:: python

      >>> product_dshape = """
      ... var * {
      ...     product_no: int32,
      ...     product_sku: string,
      ...     name: ?string,
      ...     price: ?float64
      ... }
      ... """
      >>> products = resource(
      ...     'sqlite:///%s::products' % fn,
      ...     dshape=product_dshape,
      ...     primary_key=['product_no', 'product_sku']
      ... )
      >>> orders_dshape = """
      ... var * {
      ...   order_id: int32,
      ...   product_no: map[int32, T],
      ...   quantity: ?int32
      ... }
      ... """
      >>> orders = resource(
      ...     'sqlite:///%s::orders' % fn,
      ...     dshape=orders_dshape,
      ...     primary_key=['order_id'],
      ...     foreign_keys={
      ...         'product_no': products.c.product_no
      ...         # no reference to product_sku, okay for sqlite, but not postgres
      ...     }
      ... )

Here we see that when the ``orders`` table is constructed, only one of the
columns contained in the primary key of the ``products`` table is included.

SQLite is an example of one database that allows this. Other databases such as
PostgreSQL will raise an error if the table containing the foreign keys doesn't
have a reference to all of the columns of the compound primary key.

Odo has no opinion on this, so if the database allows it, then odo will allow
it. **This is an intentional choice**.

However, it can also lead to confusing situations where something works with
SQLite, but not with PostgreSQL. These are not bugs in odo, they are an
explicit choice to allow flexibility with potentially large already-existing
systems.

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
