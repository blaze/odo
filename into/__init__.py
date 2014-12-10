from __future__ import absolute_import, division, print_function

from .convert import convert
from .append import append
from .create import create
from .resource import resource
from .into import into
from datashape import discover

try:
     from .backends.pandas import discover
except:
    pass
try:
     from .backends import bcolz
except:
    pass
try:
     from .backends import h5py
except:
    pass
try:
     from .backends import dynd
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
     from .backends import csv
except:
    pass


__version__ = '0.1.0'
