Drop
====

The ``into.drop`` function deletes a data resource.  That data resource may
live outside of Python.


Examples
--------

.. code-block:: python

   >>> from into import drop
   >>> drop('myfile.csv')                 # Removes file
   >>> drop('sqlite:///my.db::accounts')  # Drops table 'accounts'
   >>> drop('myfile.hdf5::/data/path')    # Deletes dataset from file
   >>> drop('myfile.hdf5')                # Deletes file
