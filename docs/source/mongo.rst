Mongo
=====

Odo interacts with Mongo databases through PyMongo.


URIs
----

Simple and complex examples of MongoDB uris::

    mongodb://localhost/mydb::mycollection
    mongodb://user:password@localhost:port/mydb::mycollection


Conversions
-----------

The default path in and out of a Mongo database is to use the PyMongo library
to produce and consume iterators of Python dictionaries.::

    pymongo.Collection <-> Iterator
