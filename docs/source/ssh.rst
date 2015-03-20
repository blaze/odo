SSH
===

Odo interacts with remote data over ``ssh`` using the ``paramiko`` library.

URIs
----

SSH uris consist of the ``ssh://`` protocol, a hostname, and a filename.
Simple and complex examples follow::

    ssh://hostname:myfile.csv
    ssh://username@hostname:/path/to/myfile.csv

Additionally you may want to pass authentication information through keyword
arguments to the ``odo`` function as in the following example

.. code-block:: python

   >>> from odo import odo
   >>> odo('localfile.csv', 'ssh://hostname:myfile.csv',
   ...     username='user', key_filename='.ssh/id_rsa', port=22)

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


Constructing SSH Objects explicitly
-----------------------------------

Most users usually interact with ``odo`` using URI strings.

Alternatively you can construct objects programmatically.  SSH uses the
``SSH`` type modifier

.. code-block:: python

   >>> from odo import SSH, CSV, JSON
   >>> auth = {'user': 'ubuntu',
   ...         'host': 'hostname',
   ...         'key_filename': '.ssh/id_rsa'}
   >>> data = SSH(CSV)('data/accounts.csv', **auth)
   >>> data = SSH(JSONLines)('accounts.json', **auth)

Conversions
-----------

We're able to convert any text type (``CSV, JSON, JSONLines, TextFile``) to its
equivalent on the remote server (``SSH(CSV), SSH(JSON), ...``).::


    SSH(*) <-> *

The network also allows conversions from other types, like a pandas
``DataFrame`` to a remote CSV file, by routing through a temporary local csv
file.::

    Foo <-> Temp(*) <-> SSH(*)
