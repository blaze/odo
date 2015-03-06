Odo: Shapeshifting for your data
================================

``odo`` takes two arguments, a target and a source for a data transfer.

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

   overview
   uri
   datashape
   drop


Formats
```````

.. toctree::
   :maxdepth: 1

   csv
   json
   hdf5
   sql
   mongo
   ssh
   hdfs
   aws
   spark


Developer documentation
```````````````````````

.. toctree::
   :maxdepth: 1

   type-modifiers
   functions

Odo is part of the Open Source Blaze_ projects supported by `Continuum Analytics`_

.. _Blaze: http://continuum.io/open-source/blaze/
.. _`Continuum Analytics`: http://continuum.io
