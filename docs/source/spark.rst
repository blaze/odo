Spark/SparkSQL
==================

Dependencies
------------

* `spark <https://spark.apache.org/docs/1.2.0/index.html>`_
* `pyhive <https://github.com/dropbox/PyHive>`_
* `sqlalchemy <http://docs.sqlalchemy.org/en/rel_0_9>`_

Setup
-----

We recommend you install Spark via ``conda`` from the ``blaze``
`binstar <http://www.binstar.org>`_ channel:

.. code-block:: shell

   $ conda install pyhive spark -c blaze

Interface
---------

Spark diverges a bit from other areas of ``into`` due to the way it works. With
Spark, all objects are attached to a special object called ``SparkContext``.
There can only be *one* of these running at a time. SparkSQL objects all live
inside of *one or more* ``SQLContext`` objects. ``SQLContext`` objects must be
attached to a ``SparkContext``.

Here's an example of how to setup a ``SparkContext``:

.. code-block:: python

   >>> from pyspark import SparkContext
   >>> sc = SparkContext('app', 'local')


Now we create a ``SQLContext``:


.. code-block:: python

   >>> from pyspark.sql import SQLContext
   >>> sql = SQLContext(sc)  # from the previous code block


From here, you can start adding tables to your newly created ``SQLContext``:

.. code-block:: python

   >>> data = [('Alice', 300.0), ('Bob', 200.0), ('Donatello', -100.0)]
   >>> srdd = into(sql, data, dshape='var * {name: string, amount: float64}')
   >>> type(srdd)
   <class 'pyspark.sql.SchemaRDD'>


Note the type of ``srdd``. Usually ``into(A, B)`` will return an instance of
``A`` if ``A`` is a ``type``. With Spark and SparkSQL, we need to attach whatever
we make to a context, so we "append" to an existing ``SparkContext``/``SQLContext``.

This functionality is nascent, so try it out and don't hesitate to
`report a bug or request a feature <https://github.com/ContinuumIO/into/issues/new>`_!


URIs
----
URI syntax isn't currently implemented for Spark objects.


TODO
----
* Resource/URIs
* Native loaders for JSON
* HDFS integration
