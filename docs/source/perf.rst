Loading CSVs into SQL Databases
===============================

When faced with the problem of loading a larger-than-RAM CSV into a SQL
database from within Python, many people will jump to pandas. The workflow goes
something like this:

.. code-block:: python

   import sqlalchemy as sa
   import pandas as pd
   con = sa.create_engine('mysql+pymysql://localhost/db::table')
   chunks = pd.read_csv('filename.csv', chunksize=100000)
   for chunk in chunks:
       chunk.to_sql(name='table', if_exist='append', con=con)

There is an unnecessary amount of data conversion going on here. First we
convert our CSV into an iterator of DataFrames, and then those are converted
into Python data structures compatible with SQLAlchemy. There's an enormous
cost to this process.

Why don't we use the software that was designed explicitly for this purpose?

Loading CSV files into databases is a solved problem. Databases such as
PostgreSQL solve it well. Instead of rolling our own loader every time we need
to do this and wasting computational resources, we should use the native
loaders in the database of our choosing.

Odo uses the native CSV loading capabilities of the databases it supports.
These loaders are extremely performant. Odo will beat any other pure Python
approach when loading large datasets. The following is a performance comparison
of loading the entire NYC taxi trip and fare combined dataset (about 33GB of
text) into PostgreSQL, MySQL, and SQLite3 using odo. Finally we compare a naive
approach using pandas chunked ``pd.read_csv`` and calling ``DataFrame.to_sql``
to append to the table.

PostgreSQL (22m 64s)
````````````````````

* READS: ~50 MB/s
* WRITES: ~50 MB/s

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
disabling write-ahead logging and buffering. Odo doesn't use this (yet) because
the installation requires several steps.

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


* READS: ~30 MB/s
* WRITES: ~150 MB/s

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
* TODO


Final Thoughts
``````````````
For getting CSV files into the major open source databases from within Python,
nothing will beat odo, since it's using the native capabilities of the
underlying database.
