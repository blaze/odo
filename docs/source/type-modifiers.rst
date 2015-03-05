Type Modifiers
==============

Odo decides what conversion functions to run based on the type (e.g.
``pd.DataFrame, sqlalchemy.Table, odo.CSV`` of the input.  In many cases we
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


``chunks``
----------

A particularly important type modifier is ``chunks``, which signifies an
iterable of some other type.  For example ``chunks(list)`` means an iterable of
Python lists and ``chunks(pd.DataFrame)`` an iterable of DataFrames.  The
``chunks`` modifier is often used to convert between two out-of-core formats
via an in-core format.  This is also a nice mechanism to interact with data in
an online fashion

.. code-block:: Python

   >>> from odo import odo, pd, chunks
   >>> seq = odo('postgresql://localhost::mytable', chunks(pd.DataFrame))
   >>> for df in seq:
   ...    # work on each dataframe sequentially
