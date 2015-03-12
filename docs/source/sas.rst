SAS
===

Odo interacts with local sas7bdat files through sas7bdat_.


URIs
----

SAS URI's are their paths/filenames

Simple examples of SAS uris::

    myfile.sas7bdat
    /path/to/myfile.sas7bdat


Conversions
-----------

The default paths out of SAS files is through Python iterators and Pandas DataFrames.

    SAS7BDAT -> Iterator
    SAS7BDAT -> pd.DataFrame

This is a closed file format with nice but incomplete support from the
sas7bdat_ Python library.  You should not expect comprehensive coverage.

.. _sas7bdat: https://pypi.python.org/pypi/sas7bdat
