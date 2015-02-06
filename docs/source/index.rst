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


Contents
--------

.. toctree::
   :maxdepth: 1

   overview
   uri
   csv
   json
   sql
   ssh
   hdfs
