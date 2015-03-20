Data Types
==========

We can resolve errors and increase efficiency by explicitly specifying data
types.  Odo uses DataShape_ to specify datatypes across all of the formats
that it supports.

First we motivate the use of datatypes with two examples, then we talk about
how to use DataShape.

Datatypes prevent errors
------------------------

Consider the following CSV file::

    name,balance
    Alice,100
    Bob,200
    ...
    <many more lines>
    ...
    Zelda,100.25

When ``odo`` loads this file into a new container (DataFrame, new SQL Table,
etc.) it needs to know the datatypes of the source so that it can create a
matching target.  If the CSV file is large then it looks only at the first few
hundred lines and guesses a datatype from that.  In this case it might
incorrectly guess that the balance column is of integer type because it doesn't
see a decimal value until very late in the file with the line ``Zelda,100.25``.
This will cause ``odo`` to create a target with the wrong datatypes which will
foul up the transfer.

Odo will err unless we provide an explicit datatype.  So we had this
datashape::

    var * {name: string, balance: int64)

But we want this one::

    var * {name: string, balance: float64)


Datatypes increase efficiency
-----------------------------

If we move that same CSV file into a binary store like HDF5 then we can
significantly increase efficiency if we use fixed-length strings rather than
variable length.  So we might choose to push all of the names into strings of
length ``100`` instead of leaving their lengths variable.  Even with the wasted
space this is often more efficient.  Good binary stores can often compress away
the added space but have trouble managing things of indeterminate length.

So we had this datashape::

    var * {name: string, balance: float64}

But we want this one::

    var * {name: string[100], balance: float64}


What is DataShape?
------------------

Datashape is a datatype system that includes scalar types::

    string, int32, float64, datetime, ...

Option / missing value types::

    ?string, ?int32, ?float64, ?datetime, ...

Fixed length Collections::

    10 * int64

Variable length Collections::

    var * int64

Record types::

    {name: string, balance: float64}

And any composition of the above::

    10 * 10 * {x: int32, y: int32}

    var * {name: string,
           payments: var * {when: ?datetime, amount: float32}}


DataShape and ``odo``
---------------------

If you want to be explicit you can add a datashape to an ``odo`` call with the
``dshape=`` keyword

.. code-block:: python

    >>> odo('accounts.csv', pd.DataFrame,
    ...      dshape='var * {name: string, balance: float64}')

This removes all of the guesswork from the ``odo`` heuristics.  This can
be necessary in tricky cases.


Use ``discover`` to get approximate datashapes
----------------------------------------------

We rarely write out a full datashape by hand.  Instead, use the ``discover``
function to get the datashape of an object.

.. code-block:: python

   >>> import numpy as np
   >>> from odo import discover

   >>> x = np.ones((5, 6), dtype='f4')
   >>> discover(x)
   dshape("5 * 6 * float32")

In self describing formats like numpy arrays this datashape is guaranteed to be
correct and will return very quickly.  In other cases like CSV files this
datashape is only a guess and might need to be tweaked.

.. code-block:: python

   >>> from odo import odo, resource, discover
   >>> csv = resource('accounts.csv')  # Have to use resource to discover URIs
   >>> discover(csv)
   dshape("var * {name: string, balance: int64")

   >>> ds = dshape("var * {name: string, balance: float64")  # copy-paste-modify
   >>> odo('accounts.csv', pd.DataFrame, dshape=ds)

In these cases we can copy-paste the datashape and modify the parts that we
need to change.  In the example above we couldn't call discover directly on the
URI, ``'accounts.csv'`` so instead we called ``resource`` on the URI first.
Discover returns the datashape ``string`` on all strings, regardless of whether
or not we intend them to be URIs.

Learn More
----------

DataShape is a separate project from ``odo``.  You can learn more about it
at http://datashape.pydata.org/


.. _DataShape: http://datashape.pydata.org/
