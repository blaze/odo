Type Modifiers
==============

Into decides what conversion functions to run based on the type (e.g.
``pd.DataFrame, sqlalchemy.Table, into.CSV`` of the input.  In many cases we
want slight variations to signify different circumstances such as the
difference between the following CSV files

*  A local CSV file
*  A sequence of CSV files
*  A CSV file on a remote machine
*  A CSV file on HDFS
*  A CSV file on S3
*  A temporary CSV file that should be deleted when we're done

In principle we need to create subclasses for each of these and for their
``JSON``, ``TextFile``, etc. equivalents.  To assist with this we create
functions to create these subclasses for us.  These functions are named the
following::

    chunks - a sequence of data in chunks
    SSH - data living on a remote machine
    HDFS - data living on Hadoop File system
    S3 - data living on Amazon's S3
    Directory - a directory of data
    Temp - a temporary piece of data to be garbage collected

We use these functions on *types* to construct new types.

.. code-block:: python

   >>> SSH(CSV)('/path/to/data', delimiter=',', user='ubuntu')
   >>> Directory(JSON)('/path/to/data/')

We compose these functions to specify more complex situations like a temporary
directory of JSON data living on S3

.. code-block:: python

   >>> Temp(S3(Directory(JSONLines)))

Use URIs
--------

Most users don't interact with these types.  They are for internal use by
developers to specify the situations in which a function should be called.
