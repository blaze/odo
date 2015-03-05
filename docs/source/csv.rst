CSV
===

Odo interacts with local CSV files through Pandas.


URIs
----

CSV URI's are their paths/filenames

Simple examples of CSV uris::

    myfile.csv
    /path/to/myfile.csv.gz


Keyword Arguments
-----------------

The standard csv dialect terms are usually supported::

    has_header=True/False/None
    encoding

    delimiter
    doublequote
    escapechar
    lineterminator
    quotechar
    quoting
    skipinitialspace

However these or others may be in effect depending on what library is
interacting with your file.  Oftentimes this is the ``pandas.read_csv``
function, which has an extensive `list of keyword arguments`_


Conversions
-----------

The default paths in and out of CSV files is through Pandas DataFrames.
Because CSV files might be quite large it is dangerous to read them directly
into a single DataFrame.  Instead we convert them to a stream of medium sized
DataFrames.  We call this type ``chunks(DataFrame)``.::

    chunks(DataFrame) <-> CSV

CSVs can also be efficiently loaded into SQL Databases::

    CSV -> SQL

.. _`list of keyword arguments`: http://pandas.pydata.org/pandas-docs/stable/generated/pandas.io.parsers.read_csv.html
