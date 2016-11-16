Overview
========

Odo migrates between many formats.  These include
in-memory structures like ``list``, ``pd.DataFrame`` and ``np.ndarray`` and
also data outside of Python like CSV/JSON/HDF5 files, SQL databases,
data on remote machines, and the Hadoop File System.

The ``odo`` function
--------------------

``odo`` takes two arguments, a source and a target for a data transfer.

.. code-block:: python

   >>> from odo import odo
   >>> odo(source, target)  # load source into target

It efficiently migrates data from the source to the target.

The target and source can take on the following forms

.. table::

   ====== ====== ======================================================
   Source Target Example
   ====== ====== ======================================================
   Object Object An instance of a ``DataFrame`` or ``list``
   String String ``'file.csv'``, ``'postgresql://hostname::tablename'``
          Type   ``list``, ``DataFrame``
   ====== ====== ======================================================

So the following lines would be valid inputs to ``odo``

.. code-block:: python

   >>> odo(df, list)  # create new list from Pandas DataFrame
   >>> odo(df, [])  # append onto existing list
   >>> odo(df, 'myfile.json')  # Dump dataframe to line-delimited JSON
   >>> odo('myfiles.*.csv', Iterator) # Stream through many CSV files
   >>> odo(df, 'postgresql://hostname::tablename')  # Migrate dataframe to Postgres
   >>> odo('myfile.*.csv', 'postgresql://hostname::tablename')  # Load CSVs to Postgres
   >>> odo('postgresql://hostname::tablename', 'myfile.json') # Dump Postgres to JSON
   >>> odo('mongodb://hostname/db::collection', pd.DataFrame) # Dump Mongo to DataFrame

.. warning::

   If the target in ``odo(source, target)`` already exists, it must be of a type that
   supports in-place append.

   .. code-block:: python

      >>> odo('myfile.csv', df) # this will raise TypeError because DataFrame is not appendable


Enabling Conversions
--------------------

Odo is dependent on external libraries for many of its conversions. Since most
users will only use a small subset of conversions, Odo does not install most
external libraries.

If you try to use a supported conversion and that conversion is not installed,
you may get the following error:

.. code-block::

   NotImplementedError: Unable to parse uri to data resource...

To install various subsystems of odo you can use extra install targets like:

.. code-block::

   pip install odo[postgres]
   pip install odo[bcolz]
   ...

There are a lot of these, but two special extras targets are ``odo[all]`` and
``odo[ci]``. ``odo[all]`` will install all of the subsystems. ``odo[ci]`` will
install the versions of packages we used to run the full test suite for the
release. This can be helpful if you are seeing an issue that you suspect may be
due to an incomptatible library version.


Network Effects
---------------

To convert data any pair of formats ``odo.odo`` relies on a network of
pairwise conversions.  We visualize that network below

.. figure:: images/conversions.png
   :width: 60 %
   :alt: odo network of conversions
   :target: _images/conversions.png


   Each node represents a data format. Each directed edge represents a function
   to transform data between two formats. A single call to ``odo`` may
   traverse multiple edges and multiple intermediate formats.  Red nodes
   support larger-than-memory data.

A single call to ``odo`` may traverse several intermediate formats calling on
several conversion functions.  These functions are chosen because they are
fast, often far faster than converting through a central serialization format.
