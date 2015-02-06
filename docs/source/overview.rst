Overview
========

Into migrates between many formats.  These include
in-memory structures like ``list``, ``pd.DataFrame`` and ``np.ndarray`` and
also data outside of Python like CSV/JSON/HDF5 files, SQL databases,
data on remote machines, and the Hadoop File System.

The ``into`` function
---------------------

``into`` takes two arguments, a target and a source for a data transfer.

.. code-block:: python

   >>> from into import into
   >>> into(target, source)  # load source into target

It efficiently migrates data from the source to the target.

The target and source can take on the following forms

.. raw:: html

   <table>
       <thead>
       <tr>
           <td> Target </td>
           <td> Source </td>
           <td> Example </td>
       </tr>
       </thead>
       <tr>
           <td> Object </td>
           <td> Object </td>
           <td> A particular DataFrame or list </td>
       </tr>
       <tr>
           <td> String </td>
           <td> String </td>
           <td> 'file.csv', 'postgresql://hostname::tablename' </td>
       </tr>
       <tr>
           <td> Type </td>
           <td>        </td>
           <td> Like list or pd.DataFrame </td>
       </tr>
   </table>

So the following lines would be valid inputs to ``into``

.. code-block:: python

   >>> into(list, df)  # create new list from Pandas DataFrame
   >>> into([], df)  # append onto existing list
   >>> into('myfile.json', df)  # Dump dataframe to line-delimited JSON
   >>> into(Iterator, 'myfiles.*.csv') # Stream through many CSV files
   >>> into('postgresql://hostname::tablename', df)  # Migrate dataframe to Postgres
   >>> into('postgresql://hostname::tablename', 'myfile.*.csv')  # Load CSVs to Postgres
   >>> into('myfile.json', 'postgresql://hostname::tablename') # Dump Postgres to JSON
   >>> into(pd.DataFrame, 'mongodb://hostname/db::collection') # Dump Mongo to DataFrame


Network Effects
---------------

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
