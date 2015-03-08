AWS
===

Dependencies
------------

* `boto <http://boto.readthedocs.org>`_
* `sqlalchemy <http://docs.sqlalchemy.org/en/rel_0_9>`_
* `psycopg2 <http://initd.org/psycopg>`_
* `redshift_sqlalchemy <https://github.com/cpcloud/redshift_sqlalchemy>`_

Setup
-----

First, you'll need some AWS credentials. Without these you can only access
public S3 buckets. Once you have those, S3 interaction will work. For other
services such as Redshift, the `setup is a bit more involved <http://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html>`_.

Once you have some AWS credentials, you'll need to put those in a config file.
Boto has a nice `doc page <http://boto.readthedocs.org/en/latest/boto_config_tut.html>`_
on how to set this up.

Now that you have a boto config, we're ready to interact with AWS.

Interface
---------

``odo`` provides access to the following AWS services:

* `S3 <http://aws.amazon.com/s3>`_ via boto.
* `Redshift <http://aws.amazon.com/redshift>`_ via a `SQLAlchemy dialect <https://github.com/cpcloud/redshift_sqlalchemy>`_

URIs
----

To access an S3 bucket, simply provide the path to the S3 bucket prefixed with
``s3://``

    .. code-block:: python

       >>> csvfile = resource('s3://bucket/key.csv')

Accessing a Redshift database is the same as accessing it through SQLAlchemy

    .. code-block:: python

       >>> db = resource('redshift://user:pass@host:port/database')


To access an individual table simply append ``::`` plus the table name


    .. code-block:: python

       >>> table = resource('redshift://user:pass@host:port/database::table')


Conversions
-----------

``odo`` can take advantage of Redshift's fast S3 ``COPY`` command. It works
transparently. For example, to upload a local CSV file called ``users.csv`` to a
Redshift table

    .. code-block:: python

       >>> table = odo('users.csv', 'redshift://user:pass@host:port/db::users')


Remember that these are just additional nodes in the ``odo`` network, and as
such, they are able to take advantage of conversions to types that don't have
an explicit path defined for them. This allows us to do things like convert an
S3 CSV to a pandas DataFrame

    .. code-block:: python

       >>> import pandas as pd
       >>> from odo import odo
       >>> df = odo('s3://mybucket/myfile.csv', pd.DataFrame)


TODO
----
* Multipart uploads for huge files
* GZIP'd files
* JSON to Redshift (JSONLines would be easy)
* boto ``get_bucket`` hangs on Windows
