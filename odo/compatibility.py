from __future__ import absolute_import, division, print_function

import itertools
import operator as op
import os
import shutil
import sys
import tempfile

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3
ON_TRAVIS_CI = 'TRAVIS_PYTHON_VERSION' in os.environ

if sys.version_info[0] >= 3:
    unicode = str
    map = map
    range = range
    items = op.methodcaller('items')
    TemporaryDirectory = tempfile.TemporaryDirectory
else:
    unicode = unicode
    map = itertools.imap
    range = xrange
    items = op.methodcaller('iteritems')

    class TemporaryDirectory(object):
        """Compatibility object that acts like python3's
        tempfile.TemporaryDirectory
        """
        def __init__(self, **kwargs):
            self._kwargs = kwargs
            self._path = None

        def __enter__(self):
            self._path = path = tempfile.mkdtemp(**self._kwargs)
            return path

        def __exit__(self, *excinfo):
            path = self._path
            assert path is not None, 'context was not entered'
            shutil.rmtree(self._path)


def skipif(cond, **kwargs):
    def _(func):
        if cond:
            return None
        else:
            return func
    return _

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
