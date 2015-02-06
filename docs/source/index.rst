.. into documentation master file, created by
   sphinx-quickstart on Sat Dec  6 16:03:44 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Into
====

``into`` takes two arguments, a target and a source for a data transfer.

.. code-block:: python

   >>> into(target, source)  # load source into target

It efficiently migrates data from the source to the target.


Example
-------

.. code-block:: python

   >>> from into import into
   >>> import pandas as pd

   >>> into(pd.DataFrame, 'accounts.csv')  # Load csv file into DataFrame
         name  balance
   0    Alice      100
   1      Bob      200
   2  Charlie      300

   >>> # Load CSV file into Hive database
   >>> into('hive://user:password@hostname/db::accounts', 'accounts.csv')

Into migrates between many formats.  These include
in-memory structures like ``list``, ``pd.DataFrame`` and ``np.ndarray`` and
also data outside of Python like CSV/JSON/HDF5 files, SQL databases,
data on remote machines, and the Hadoop File System.

To convert data any pair of formats ``into`` relies on a network of
pairwise conversions.  We visualize that network below

.. figure:: images/conversions.png
   :width: 60 %
   :alt: into network of conversions
   :target: _images/conversions.png


   Each node represents a data format. Each directed edge represents a function
   to transform data between two formats. A single call to ``into`` may
   traverse multiple edges and multiple intermediate formats.  Red nodes
   support larger-than-memory data.

A single call to ``into`` may traverse several intermediate formats calling on
several conversion functions.  These functions are chosen because they are
fast, often far faster than converting through a central serialization format.

Contents:

.. toctree::
   :maxdepth: 1

   uri
   csv
   json
   sql
   ssh



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
