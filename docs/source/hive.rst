Hive Metastore
==============

The Hive metastore relates SQL metadata to files on the Hadoop File System
(HDFS_).  It is similar to a SQL database in that it contains information about
SQL tables but dissimilar in that data isn't stored in Hive but remains
ordinary files on HDFS.

Odo interacts with Hive mostly through ``sqlalchemy`` and also with a bit
of custom code due to its peculiarities.

URIs
----

Hive uris match exactly SQLAlchemy connection strings with the ``hive://``
protocol.  Additionally, Impala, another SQL database on HDFS can also connect
to the same tables.
::

    hive://hostname
    hive://user@hostname:port/database-name
    hive://user@hostname:port/database-name::table-name

    impala://hostname::table-name

Additionally you should probably inspect docs on HDFS_ due to the tight
integration between the two.

Options
-------

Hive tables have a few non-standard options on top of normal SQL::

    stored_as - File format on disk like TEXTFILE, PARQUET, ORC
    path - Absolute path to file location on HDFS
    external=True - Whether to keep the file external or move it to Hive
        directory

See `Hive DDL`_ docs for more information.


Conversions
-----------

We commonly load CSV files in to Hive, either from HDFS or from local disk on
one of the machines that comprise the HDFS cluster::

    HDFS(Directory(CSV)) -> Hive
    SSH(Directory(CSV)) -> Hive
    SSH(CSV) -> Hive

Additionally we can use Hive to efficiently migrate this data to new data in a
different format::

    Hive -> Hive

And as with all SQL systems through SQLAlchemy we can convert a Hive table to a
Python Iterator, though this is somewhat slow::

    Hive -> Iterator


Impala
------

Impala operates on the same data as Hive, is generally faster, though also has
a couple of quirks.

While Impala connects to the same metastore it must connect to one of the
worker nodes, not the same head node to which Hive connects.  After you load
data in to hive you need to send the ``invalidate metadata`` to Impala.

.. code-block:: python

   >>> odo('hdfs://hostname::/path/to/data/*.csv', 'hive://hostname::table')

   >>> imp = resource('impala://workernode')
   >>> imp.connect().execute('invalidate metadata')

This is arguably something that ``odo`` should handle in the future.  After
this, all tables in Hive are also available to Impala.

You may want to transform your data in to Parquet format for efficient
querying.  A two minute query on Hive in CSV might take one minute on Hive in
Parquet and only three seconds in Impala in Parquet.

.. code-block:: python

   >>> odo('hive://hostname::table', 'hive://hostname::table_parquet',
   ...     external=False, stored_as='PARQUET')


.. _HDFS: hdfs.html
.. _`Hive DDL`: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL
