SQL
===

Odo interacts with SQL databases through SQLAlchemy.  As a result, ``odo``
supports all databases that SQLAlchemy supports.  Through third-party
extensions, SQLAlchemy supports *most* databases.

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

Enabling Conversions
--------------------

Odo is dependent on various conversion libraries.  Many of these are not 
installed by default. 

For example the SQLAlchemy library is used for sqlite, PostgreSQL, MySQL, Hive,
and RedShift conversions.  SQLAlchemy is not installed by default, therefore 
you must install it if you want to use one of these databases.

You can install SQLAlchemy by typing one of shell commands below::

    $ conda install sqlalchemy
    or
    $ pip install sqlalchemy

If a conversion is not enabled you may get the following error::

    NotImplementedError: Unable to parse uri to data resource: ...
