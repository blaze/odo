Performance
===========

Loading CSVs into SQL Databases
-------------------------------

Odo uses the native CSV loading capabilities of the databases it supports.
These loaders are well tested and high performing. Odo will beat any other pure
Python approach when loading large datasets. The following is a performance
comparison of loading the entire NYC taxi trip and fare combined dataset into
PostgreSQL, MySQL, and SQLite3 using odo. Finally we compare a naive approach
using pandas chunked ``read_csv`` and calling ``to_sql`` to append to the table


PostgreSQL (22m 64s)
````````````````````

READS: ~50 MB/s
WRITES: ~50 MB/s

The ``COPY`` command built into postgresql is quite fast. Odo generates code
for the ``COPY`` command using a custom SQLAlchemy expression.

.. code-block:: python

   In [1]: %time t = odo('all.csv', 'postgresql://localhost::nyc')
   CPU times: user 1.43 s, sys: 330 ms, total: 1.76 s
   Wall time: 22min 46s

``pg_bulkload`` Command Line Utility (13m 17s)
``````````````````````````````````````````````

* READS: ~50 MB/s
* WRITES: ~50 MB/s

A special command line tool called ``pg_bulkload`` exists solely for the
purpose of loading files into a postgresql table. It achieves its speedups by
disabling write-ahead logging and buffering.

.. code-block:: sh

   $ time ./pg_bulkload nyc2.ctl < all.csv
   NOTICE: BULK LOAD START
   NOTICE: BULK LOAD END
           1 Rows skipped.
           173179759 Rows successfully loaded.
           0 Rows not loaded due to parse errors.
           0 Rows not loaded due to duplicate errors.
           0 Rows replaced with new rows.
   ./pg_bulkload nyc2.ctl < all.csv  26.14s user 33.31s system 7% cpu 13:17.31 total

MySQL (21m 47s)
```````````````

.. code-block:: python

   In [1]: %time t = odo('all.csv', 'mysql+pymysql://localhost/test::nyc')


READS: ~30 MB/s
WRITES: ~150 MB/s

SQLite3 (57m 31s\*)
```````````````````

.. code-block:: python

   In [1]: dshape = discover(resource('all.csv'))

   In [2]: %time t = odo('all.no.header.csv', 'sqlite:///db.db::nyc', dshape=dshape)
   CPU times: user 3.09 s, sys: 819 ms, total: 3.91 s
   Wall time: 57min 31s

\* Here, we call ``discover`` on a version of the dataset that has the header in
the first line and we use a version of the dataset *without* the header line in
the sqlite3 ``.import`` command. This is sort of cheating, but I wanted to see
what the loading time of sqlite3's import command was without the overhead of
creating a new file sans the header line.

Pandas
``````

For getting CSV files into the major open source databases, nothing will beat
odo, since it's using the native capabilities of the underlying database.
