from __future__ import absolute_import as _
from __future__ import division as _
from __future__ import print_function as _

try:
    # h5py has precedence over pytables
    import h5py as _
except ImportError:
    pass

from multipledispatch import halt_ordering as _halt_ordering
from multipledispatch import restart_ordering as _restart_ordering

# Turn off multipledispatch ordering
_halt_ordering()

from .convert import convert
from .append import append
from .resource import resource
from .into import into
from .odo import odo
from .create import create
from .drop import drop
from datashape import discover, dshape

from . import backends


# Restart multipledispatch ordering and do ordering
_restart_ordering()

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
