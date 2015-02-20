Into
====

|Build Status| |Doc Status|

Data migration in Python

Documentation_

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

Into migrates data using network of small data conersion functions between
type pairs. That network is below:

.. image:: https://raw.githubusercontent.com/ContinuumIO/into/master/docs/source/images/conversions.png
   :alt: into conversions

Each node is a container type (like ``pandas.DataFrame`` or
``sqlalchemy.Table``) and each directed edge is a function that transforms or
appends one container into or onto another.  We annotate these functions/edges
with relative costs.

This network approach allows ``into`` to select the shortest path between any
two types (thank you networkx_).  For performance reasons these functions often
leverage non-Pythonic systems like NumPy arrays or native ``CSV->SQL`` loading
functions.  Into is not dependent on only Python iterators.

This network approach is also robust.  When libraries go missing or runtime
errors occur ``into`` can work around these holes and find new paths.

This network approach is extensible.  It is easy to write small functions and
register them to the overall graph.  In the following example showing how we
convert from ``pandas.DataFrame`` to a ``numpy.ndarray``.

.. code-block:: python

   from into import convert

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
* ``into``: Call one of the above based on inputs.
  E.g. ``into(list, (1, 2, 3)) -> convert(list, (1, 2, 3))``
  while ``L = []; into(L, (1, 2, 3)) -> append(L, (1, 2, 3))``

Finally, ``into`` is also aware of which containers must reside in memory and
which do not.  In the graph above the *red-colored* nodes are robust to
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
.. _Documentation: https://into.readthedocs.org/en/latest/
.. |Build Status| image:: https://travis-ci.org/ContinuumIO/into.png
   :target: https://travis-ci.org/ContinuumIO/into
.. |Doc Status| image:: https://readthedocs.org/projects/into/badge/?version=latest
   :target: https://readthedocs.org/projects/into/?badge=latest
   :alt: Documentation Status
