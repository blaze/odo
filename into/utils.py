from __future__ import absolute_import, division, print_function

from contextlib import contextmanager
import inspect
import tempfile
import os


def raises(err, lamda):
    try:
        lamda()
        return False
    except err:
        return True


def expand_tuples(L):
    """

    >>> expand_tuples([1, (2, 3)])
    [(1, 2), (1, 3)]

    >>> expand_tuples([1, 2])
    [(1, 2)]
    """
    if not L:
        return [()]
    elif not isinstance(L[0], tuple):
        rest = expand_tuples(L[1:])
        return [(L[0],) + t for t in rest]
    else:
        rest = expand_tuples(L[1:])
        return [(item,) + t for t in rest for item in L[0]]

@contextmanager
def tmpfile(extension=''):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension)

    yield filename

    try:
        if os.path.exists(filename):
            os.remove(filename)
    except OSError:  # Sometimes Windows can't close files
        if os.name == 'nt':
            os.close(handle)
            try:
                os.remove(filename)
            except OSError:  # finally give up
                pass


def keywords(func):
    """ Get the argument names of a function

    >>> def f(x, y=2):
    ...     pass

    >>> keywords(f)
    ['x', 'y']
    """
    if isinstance(func, type):
        return keywords(func.__init__)
    return inspect.getargspec(func).args


def cls_name(cls):
    if 'builtin' in cls.__module__:
        return cls.__name__
    else:
        return cls.__module__.split('.')[0] + '.' + cls.__name__


@contextmanager
def filetext(text, extension='', open=open, mode='wt'):
    with tmpfile(extension=extension) as filename:
        f = open(filename, mode=mode)
        try:
            f.write(text)
        finally:
            try:
                f.close()
            except AttributeError:
                pass

        yield filename
