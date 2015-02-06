CSV
===

Into interacts with local CSV files through Pandas.


URIs
----

CSV URI's are their paths/filenames

Simple examples of CSV uris::

    myfile.csv
    /path/to/myfile.csv.gz


Conversions
-----------

The default paths in and out of CSV files is through Pandas DataFrames.
Because CSV files might be quite large it is dangerous to read them directly
into a single DataFrame.  Instead we convert them to a stream of medium sized
DataFrames.  We call this type ``chunks(DataFrame)``.
