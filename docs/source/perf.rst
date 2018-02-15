Loading CSVs into SQL Databases
===============================

When faced with the problem of loading a larger-than-RAM CSV into a SQL
database from within Python, many people will jump to pandas. The workflow goes
something like this:

.. code-block:: python

   >>> import sqlalchemy as sa
   >>> import pandas as pd
   >>> con = sa.create_engine('postgresql://localhost/db')
   >>> chunks = pd.read_csv('filename.csv', chunksize=100000)
   >>> for chunk in chunks:
   ...     chunk.to_sql(name='table', if_exist='append', con=con)

There is an unnecessary and very expensive amount of data conversion going on
here. First we convert our CSV into an iterator of DataFrames, then those
DataFrames are converted into Python data structures compatible with
SQLAlchemy. Those Python objects then need to be serialized in a way that's
compatible with the database they are being sent to. Before you know it, more
time is spent converting data and serializing Python data structures than on
reading data from disk.

Use the technology that has already solved your problem well
------------------------------------------------------------

Loading CSV files into databases is a solved problem. It's a problem that has
been solved well. Instead of rolling our own loader every time we need to do
this and wasting computational resources, we should use the native loaders in
the database of our choosing. Odo lets you do this with a single line of code.

How does odo achieve native database loading speed?
---------------------------------------------------

Odo uses the native CSV loading capabilities of the databases it supports.
These loaders are extremely fast. Odo will beat any other pure Python approach
when loading large datasets. The following is a performance comparison of
loading the entire NYC taxi trip and fare combined dataset (about 33GB of text)
into PostgreSQL, MySQL, and SQLite3 using odo. Our baseline for comparison is
pandas.

**NB:** I'm happy to hear about other optimizations that I may not be taking
advantage of.

Timings
-------

CSV → PostgreSQL (22m 46s)
``````````````````````````
* READS: ~50 MB/s
* WRITES: ~50 MB/s

The ``COPY`` command built into postgresql is quite fast. Odo generates code
for the ``COPY`` command using a custom SQLAlchemy expression.

.. code-block:: python

   In [1]: %time t = odo('all.csv', 'postgresql://localhost::nyc')
   CPU times: user 1.43 s, sys: 330 ms, total: 1.76 s
   Wall time: 22min 46s

PostgreSQL → CSV (21m 32s)
``````````````````````````
Getting data out of the database takes roughly the same amount of time as
loading it in.


``pg_bulkload`` Command Line Utility (13m 17s)
``````````````````````````````````````````````
* READS: ~50 MB/s
* WRITES: ~50 MB/s

A special command line tool called ``pg_bulkload`` exists solely for the
purpose of loading files into a postgresql table. It achieves its speedups by
disabling WAL (write ahead logging) and buffering. Odo doesn't use this (yet)
because the installation requires several steps. There are also implications
for data integrity when turning off WAL.

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

CSV → MySQL (20m 49s)
`````````````````````

.. code-block:: python

   In [1]: %time t = odo('all.csv', 'mysql+pymysql://localhost/test::nyc')
   CPU times: user 1.32 s, sys: 304 ms, total: 1.63 s
   Wall time: 20min 49s

* READS: ~30 MB/s
* WRITES: ~150 MB/s

MySQL → CSV (17m 47s)
`````````````````````

.. code-block:: python

  In [1]: %time csv = odo('mysql+pymysql://localhost/test::nyc', 'nyc.csv')
  CPU times: user 1.03 s, sys: 259 ms, total: 1.29 s
  Wall time: 17min 47s

* READS: ~30 MB/s
* WRITES: ~30 MB/s

Similar to PostgreSQL, MySQL takes roughly the same amount of time to write a
CSV as it does to load it into a table.

CSV → SQLite3 (57m 31s\*)
`````````````````````````

.. code-block:: python

   In [1]: dshape = discover(resource('all.csv'))

   In [2]: %time t = odo('all.no.header.csv', 'sqlite:///db.db::nyc',
      ...:               dshape=dshape)
   CPU times: user 3.09 s, sys: 819 ms, total: 3.91 s
   Wall time: 57min 31s

\* Here, we call ``discover`` on a version of the dataset that has the header
in the first line and we use a version of the dataset *without* the header line
in the sqlite3 ``.import`` command. This is sort of cheating, but I wanted to
see what the loading time of sqlite3's import command was without the overhead
of creating a new file without the header line.

SQLite3 → CSV (46m 43s)
```````````````````````
* READS: ~15 MB/s
* WRITES: ~13 MB/s

.. code-block:: python

   In [1]: %time t = odo('sqlite:///db.db::nyc', 'nyc.csv')
   CPU times: user 2.7 s, sys: 841 ms, total: 3.55 s
   Wall time: 46min 43s

Pandas
``````
* READS: ~60 MB/s
* WRITES: ~3-5 MB/s

I didn't actually finish this timing because a single iteration of inserting
1,000,000 rows took about 4 minutes and there would be 174 such iterations
bringing the total loading time to:

.. code-block:: python

   >>> 175 * 4 / 60.0  # doctest: +ELLIPSIS
   11.66...

11.66 **hours**!

Nearly *12* hours to insert 175 million rows into a postgresql database. The
next slowest database (SQLite) is still **11x** faster than reading your CSV
file into pandas and then sending that ``DataFrame`` to PostgreSQL with the
``to_pandas`` method.

Final Thoughts
``````````````
For getting CSV files into the major open source databases from within Python,
nothing is faster than odo since it takes advantage of the capabilities of the
underlying database.

Don't use pandas for loading CSV files into a database.
