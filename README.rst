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

Into leverages the existing Python ecosystem.  The example above uses
``sqlalchemy`` for SQL interation and ``h5py`` for HDF5 interaction.


Method
------

Into accomplishes data migration migration through a network of small data
conversion functions It migrates data between a network of known
transformations.  That network is below:

.. image:: https://github.com/ContinuumIO/into/blob/master/doc/images/conversions.png
   :width: 400 px
   :alt: into conversions

Each node is a container type (like ``DataFrame``) and each directed edge is a
function that transforms or appends one container into or onto another.  Edges
are annotated with relative costs.  This network approach allows ``into`` to
select the shortest path between any two types (thank you networkx_).
These functions often leverage non-Pythonic systems like NumPy arrays or native
``CSV->SQL`` loading functions.  ``Into`` is not dependent on Python iterators.

This network approach is also robust.  Into can work around missing edges or
types due to lack of dependencies or runtime errors.  This network approach is
extensible.  It is easy to write small functions and register them to the
overall graph as in the following example showing how we convert from
``pandas.DataFrame`` to a ``numpy.ndarray``.

.. code-block:: python

   from into import convert

   @convert.register(np.ndarray, pd.DataFrame, cost=1.0)
   def dataframe_to_numpy(df, **kwargs):
       return df.to_records(index=False)

We decorate ``convert`` functions with the target and source types as well as a
relative cost.  This decoration establishes a contract that the underlying
function must fulfill, in this case with the ``DataFrame.to_records`` method.
Similar functions exist for ``append``, to add to existing data, and
``resource`` for URI resolution.

Finally, ``into`` is also aware of which containers must reside in memory and
which do not.  In the graph above the red-colored nodes are robust to
larger-than-memory datasets.  Transformations between two out-of-core datasets
operate only on the subgraph of the red nodes.


LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/into/blob/master/LICENSE.txt>`__.

History
-------

Into was factored out from the Blaze_ project.


.. _Blaze: http://blaze.pydata.org/
.. _networkx: https://networkx.github.io/
