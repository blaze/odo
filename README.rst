Odo
===

|Build Status| |Doc Status|

.. image:: https://binstar.org/blaze/odo/badges/build.svg
   :target: https://binstar.org/blaze/odo/builds

.. image:: https://binstar.org/blaze/odo/badges/version.svg
   :target: https://binstar.org/blaze/odo

Data migration in Python

Documentation_

Example
-------

Odo migrates data between different containers

.. code-block:: python

   >>> from odo import odo
   >>> odo((1, 2, 3), list)
   [1, 2, 3]

It operates on small, in-memory containers (as above) and large, out-of-core
containers (as below)

.. code-block:: python

   >>> odo('myfile.hdf5::/data', 'postgresql://user:pass@host::my-table')
   Table('my-table', MetaData(bind=Engine(postgresql://user:****@host)), ...)

Odo leverages the existing Python ecosystem.  The example above uses
``sqlalchemy`` for SQL interation and ``h5py`` for HDF5 interaction.


Method
------

Odo migrates data using network of small data conversion functions between
type pairs. That network is below:

.. image:: https://raw.githubusercontent.com/ContinuumIO/odo/master/docs/source/images/conversions.png
   :alt: odo conversions

Each node is a container type (like ``pandas.DataFrame`` or
``sqlalchemy.Table``) and each directed edge is a function that transforms or
appends one container into or onto another.  We annotate these functions/edges
with relative costs.

This network approach allows ``odo`` to select the shortest path between any
two types (thank you networkx_).  For performance reasons these functions often
leverage non-Pythonic systems like NumPy arrays or native ``CSV->SQL`` loading
functions.  Odo is not dependent on only Python iterators.

This network approach is also robust.  When libraries go missing or runtime
errors occur ``odo`` can work around these holes and find new paths.

This network approach is extensible.  It is easy to write small functions and
register them to the overall graph.  In the following example showing how we
convert from ``pandas.DataFrame`` to a ``numpy.ndarray``.

.. code-block:: python

   from odo import convert

   @convert.register(np.ndarray, pd.DataFrame, cost=1.0)
   def dataframe_to_numpy(df, **kwargs):
       return df.to_records(index=False)

We decorate ``convert`` functions with the target and source types as well as a
relative cost.  This decoration establishes a contract that the underlying
function must fulfill, in this case with the fast ``DataFrame.to_records``
method.  Similar functions exist for ``append``, to add to existing data, and
``resource`` for URI resolution.

* ``convert``: Transform dataset into new container
* ``append``: Add dataset onto existing container
* ``resource``: Given a URI find the appropriate data resource
* ``odo``: Call one of the above based on inputs.
  E.g. ``odo((1, 2, 3), list) -> convert(list, (1, 2, 3))``
  while ``L = []; odo((1, 2, 3), L) -> append(L, (1, 2, 3))``

Finally, ``odo`` is also aware of which containers must reside in memory and
which do not.  In the graph above the *red-colored* nodes are robust to
larger-than-memory datasets.  Transformations between two out-of-core datasets
operate only on the subgraph of the red nodes.


LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/odo/blob/master/LICENSE.txt>`__.

History
-------

Odo was factored out from the Blaze_ project.


.. _Blaze: http://blaze.pydata.org/
.. _networkx: https://networkx.github.io/
.. _Documentation: https://odo.readthedocs.org/en/latest/
.. |Build Status| image:: https://travis-ci.org/ContinuumIO/odo.png
   :target: https://travis-ci.org/ContinuumIO/odo
.. |Doc Status| image:: https://readthedocs.org/projects/odo/badge/?version=latest
   :target: https://readthedocs.org/projects/odo/?badge=latest
   :alt: Documentation Status
