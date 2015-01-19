from __future__ import absolute_import, division, print_function

__all__ = """convert append resource into drop chunks Chunks discover dshape
             CSV JSON JSONLines S3""".split()

from multipledispatch import halt_ordering, restart_ordering

halt_ordering()  # Turn off multipledispatch ordering

from .convert import convert
from .append import append
from .resource import resource
from .into import into
from .drop import drop
from .chunks import chunks, Chunks
from datashape import discover, dshape
import numpy as np

try:
     from .backends.sas import sas7bdat
except:
    pass
try:
     from .backends.pandas import pd
except ImportError:
    pass
try:
     from .backends.bcolz import bcolz
except ImportError:
    pass
try:
     from .backends.h5py import h5py
except ImportError:
    pass
try:
     from .backends.hdfstore import HDFStore
except ImportError:
    pass
try:
     from .backends.pytables import tables
except ImportError:
    pass
try:
     from .backends.dynd import nd
except ImportError:
    pass
try:
     from .backends import sql
except ImportError:
    pass
try:
     from .backends import mongo
except ImportError:
    pass
try:
     from .backends.csv import CSV
except ImportError:
    pass
try:
     from .backends.json import JSON, JSONLines
except ImportError:
    pass
try:
     from .backends import sql_csv
except ImportError:
    pass
try:
    from .backends.aws import S3
except ImportError:
    pass


restart_ordering()  # Restart multipledispatch ordering and do ordering

__version__ = '0.1.3'
