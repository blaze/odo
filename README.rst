Into
====

Data migration in Python


Example
-------

Into migrates data between different containers

.. code-block:: python

   >>> from into import into

   >>> into(list, (1, 2, 3))
   [1, 2, 3]

It operates on small, in-memory containers (as above) and large, out-of-core
containers (as below)

.. code-block:: python

   >>> into('postgresql://user:pass@host::my-table', 'myfile.hdf5::/data')
   Table('my-table', MetaData(bind=Engine(postgresql://user:****@host)), ...)

It accomplishes this through a network of small data conversion functions that
leverage the existing Python ecosystem.  The example above uses ``sqlalchemy``
for SQL interation and ``h5py`` for HDF5 interaction.  It migrates data between
a network of known transformations.  That network is below:


It accomplishes this through a network of small data conversion functions that
leverage the existing Python ecosystem.  The example above uses ``sqlalchemy``
for SQL interation and ``h5py`` for HDF5 interaction.  It migrates data between
a network of known transformations.  That network is below:


.. image:: https://github.com/ContinuumIO/into/blob/master/docs/images/conversions.png
   :width: 400 px
   :alt: into conversions


LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/into/blob/master/LICENSE.txt>`__.

History
-------

Factored out from the Blaze_ project


.. _Blaze: http://blaze.pydata.org/
