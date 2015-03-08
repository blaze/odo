JSON
====

Odo interacts with local JSON files through the standard ``json`` library.


URIs
----

JSON URI's are their paths/filenames

Simple examples of JSON uris::

    myfile.json
    /path/to/myfile.json.gz


Line Delimited JSON
-------------------

Internally ``odo`` can deal with both traditional "single blob per file" JSON
as well as line-delimited "one blob per line" JSON.  We inspect existing files
to see which format it is.  On new files we default to line-delimited however
this can be overruled by using the following protocols::

    json://myfile.json       # traditional JSON
    jsonlines://myfile.json  # line delimited JSON


Conversions
-----------

The default paths in and out of JSON files is through Python iterators of dicts.::

    JSON <-> Iterator
