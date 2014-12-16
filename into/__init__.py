from __future__ import absolute_import, division, print_function

from multipledispatch import halt_ordering, restart_ordering

halt_ordering() # Turn off multipledispatch ordering

from .convert import convert
from .append import append
from .create import create
from .resource import resource
from .into import into
from .drop import drop
from .chunks import chunks, Chunks
from datashape import discover, dshape

try:
     from .backends.pandas import pd
except:
    pass
try:
     from .backends.bcolz import bcolz
except:
    pass
try:
     from .backends.h5py import h5py
except:
    pass
try:
     from .backends.pytables import tables
except:
    pass
try:
     from .backends.dynd import nd
except:
    pass
try:
     from .backends import sql
except:
    pass
try:
     from .backends import mongo
except:
    pass
try:
     from .backends.csv import CSV
except:
    pass
try:
     from .backends import sql_csv
except:
    pass


restart_ordering() # Restart multipledispatch ordering and do ordering

__version__ = '0.1.2'
