SSH
===

Into interacts with remote data over ``ssh`` using the ``paramiko`` library.

URIs
----

SSH uris consist of the ``ssh://`` protocol, a hostname, and a filename.
Simple and complex examples follow::

    ssh://hostname:myfile.csv
    ssh://username@hostname:/path/to/myfile.csv

Additionally you may want to pass authentication information through keyword
arguments to the into function as in the following example

.. code-block:: python

   into('ssh://hostname:myfile.csv', 'localfile.csv',
        username='user', key_filename='.ssh/id_rsa', port=22)

We pass through authentication keyword arguments to the
``paramiko.SSHClient.connect`` method.  That method takes the following
options::

    port=22
    username=None
    password=None
    pkey=None
    key_filename=None
    timeout=None
    allow_agent=True
    look_for_keys=True
    compress=False
    sock=None


Conversions
-----------

We're able to convert any text type (``CSV, JSON, JSONLines, TextFile``) to its
equivalent on the remote server (``SSH(CSV), SSH(JSON), ...``).  The network
allows conversions from other types, like a pandas dataframe to a remote CSV
file, by routing through a temporary local csv file.
