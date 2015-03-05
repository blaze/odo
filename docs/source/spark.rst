Spark/SparkSQL
==============

Dependencies
------------

* `spark <https://spark.apache.org/docs/1.2.0/index.html>`_
* `pyhive <https://github.com/dropbox/PyHive>`_
* `sqlalchemy <http://docs.sqlalchemy.org/en/rel_0_9>`_

Setup
-----

We recommend you install Spark via ``conda`` from the ``blaze``
`binstar <http://www.binstar.org>`_ channel:

.. code-block:: sh

   $ conda install pyhive spark -c blaze

The package works well on Ubuntu Linux and Mac OS X. Other issues may arise
when installing this package on a non-Ubuntu Linux distro. There's a
`known issue <https://github.com/quasiben/backend-recipes/issues/1>`_ with
Arch Linux.

Interface
---------

Spark diverges a bit from other areas of ``odo`` due to the way it works. With
Spark, all objects are attached to a special object called ``SparkContext``.
There can only be *one* of these running at a time. In contrast, SparkSQL
objects all live inside of *one or more* ``SQLContext`` objects. ``SQLContext``
objects must be attached to a ``SparkContext``.

Here's an example of how to setup a ``SparkContext``:

.. code-block:: python

   >>> from pyspark import SparkContext
   >>> sc = SparkContext('app', 'local')


Next we create a ``SQLContext``:


.. code-block:: python

   >>> from pyspark.sql import SQLContext
   >>> sql = SQLContext(sc)  # from the previous code block


From here, you can start using ``odo`` to create ``SchemaRDD`` objects, which
are the SparkSQL version of a table:

.. code-block:: python

   >>> from odo import odo
   >>> data = [('Alice', 300.0), ('Bob', 200.0), ('Donatello', -100.0)]
   >>> type(sql)
   <class 'pyspark.sql.SQLContext'>
   >>> srdd = odo(data, sql, dshape='var * {name: string, amount: float64}')
   >>> type(srdd)
   <class 'pyspark.sql.SchemaRDD'>


Note the type of ``srdd``. Usually ``odo(A, B)`` will return an instance of
``B`` if ``B`` is a ``type``. With Spark and SparkSQL, we need to attach whatever
we make to a context, so we "append" to an existing ``SparkContext``/``SQLContext``.
Instead of returning the context object, ``odo`` will return the ``SchemaRDD``
that we just created. This makes it more convenient to do things with the result.

This functionality is nascent, so try it out and don't hesitate to
`report a bug or request a feature <https://github.com/ContinuumIO/into/issues/new>`_!


URIs
----
URI syntax isn't currently implemented for Spark objects.


Conversions
-----------
The main paths into and out of ``RDD`` and ``SchemaRDD`` are through Python
``list`` objects:

::

   RDD <-> list
   SchemaRDD <-> list


Additionally, there's a specialized one-way path for going directly to
``SchemaRDD`` from ``RDD``:

::

   RDD -> SchemaRDD

TODO
----
* Resource/URIs
* Native loaders for JSON and possibly CSV
* HDFS integration
