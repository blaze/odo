from .convert import convert
from .append import append
from .create import create
from .into import into

try:
     from .backends import h5py
except:
    pass
try:
     from .backends import dynd
except:
    pass
