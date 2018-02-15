Odo: Shapeshifting for your data
================================
``odo`` takes two arguments, a source and a target for a data transfer.

.. code-block:: python

   >>> from odo import odo
   >>> odo(source, target)  # load source into target

It efficiently migrates data from the source to the target through a network
of conversions.

.. figure:: images/conversions.png
   :width: 60 %
   :alt: odo network of conversions
   :target: _images/conversions.png


Example
-------

.. code-block:: python

   >>> from odo import odo
   >>> import pandas as pd

   >>> odo('accounts.csv', pd.DataFrame)  # Load csv file into DataFrame
         name  balance
   0    Alice      100
   1      Bob      200
   2  Charlie      300

   >>> # Load CSV file into Hive database
   >>> odo('accounts.csv', 'hive://user:password@hostname/db::accounts')


Contents
--------

General
```````

.. toctree::
   :maxdepth: 1

   project-info
   overview
   uri
   datashape
   drop
   perf
   add-new-backend
   releases


Formats
```````

.. toctree::
   :maxdepth: 1

   aws
   csv
   json
   hdf5
   hdfs
   hive
   mongo
   spark
   sas
   sql
   ssh


Developer Documentation
```````````````````````

.. toctree::
   :maxdepth: 1

   type-modifiers
   functions

Odo is part of the Open Source Blaze_ projects supported by `Continuum Analytics`_

.. _Blaze: http://blaze.pydata.org/
.. _`Continuum Analytics`: http://continuum.io
