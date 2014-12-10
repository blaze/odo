from __future__ import absolute_import, division, print_function

import sys
import itertools
if sys.version_info[0] >= 3:
    unicode = str
    map = map
    range = range
else:
    unicode = unicode
    map = itertools.imap
    range = xrange
