from __future__ import absolute_import, division, print_function

from datashape import dshape, Record
from toolz import pluck, get
from contextlib import contextmanager
import inspect
import datetime
import tempfile
import os
import numpy as np
from .compatibility import unicode


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
    os.remove(filename)

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


@contextmanager
def filetexts(d, open=open):
    """ Dumps a number of textfiles to disk

    d - dict
        a mapping from filename to text like {'a.csv': '1,1\n2,2'}
    """
    for filename, text in d.items():
        f = open(filename, 'wt')
        try:
            f.write(text)
        finally:
            try:
                f.close()
            except AttributeError:
                pass

    yield list(d)

    for filename in d:
        if os.path.exists(filename):
            os.remove(filename)


def normalize_to_date(dt):
    if isinstance(dt, datetime.datetime) and not dt.time():
        return dt.date()
    else:
        return dt


def assert_allclose(lhs, rhs):
    for tb in map(zip, lhs, rhs):
        for left, right in tb:
            if isinstance(left, (np.floating, float)):
                # account for nans
                assert np.all(np.isclose(left, right, equal_nan=True))
                continue
            if isinstance(left, datetime.datetime):
                left = normalize_to_date(left)
            if isinstance(right, datetime.datetime):
                right = normalize_to_date(right)
            assert left == right



def records_to_tuples(ds, data):
    """ Transform records into tuples

    Examples
    --------
    >>> seq = [{'a': 1, 'b': 10}, {'a': 2, 'b': 20}]
    >>> list(records_to_tuples('var * {a: int, b: int}', seq))
    [(1, 10), (2, 20)]

    >>> records_to_tuples('{a: int, b: int}', seq[0])  # single elements
    (1, 10)

    >>> records_to_tuples('var * int', [1, 2, 3])  # pass through on non-records
    [1, 2, 3]

    See Also
    --------

    tuples_to_records
    """
    if isinstance(ds, (str, unicode)):
        ds = dshape(ds)
    if isinstance(ds.measure, Record) and len(ds.shape) == 1:
        return pluck(ds.measure.names, data)
    if isinstance(ds.measure, Record) and len(ds.shape) == 0:
        return get(ds.measure.names, data)
    if not isinstance(ds.measure, Record):
        return data
    raise NotImplementedError()


def tuples_to_records(ds, data):
    """ Transform tuples into records

    Examples
    --------
    >>> seq = [(1, 10), (2, 20)]
    >>> list(tuples_to_records('var * {a: int, b: int}', seq))  # doctest: +SKIP
    [{'a': 1, 'b': 10}, {'a': 2, 'b': 20}]

    >>> tuples_to_records('{a: int, b: int}', seq[0])  # doctest: +SKIP
    {'a': 1, 'b': 10}

    >>> tuples_to_records('var * int', [1, 2, 3])  # pass through on non-records
    [1, 2, 3]

    See Also
    --------

    records_to_tuples
    """
    if isinstance(ds, (str, unicode)):
        ds = dshape(ds)
    if isinstance(ds.measure, Record) and len(ds.shape) == 1:
        names = ds.measure.names
        return (dict(zip(names, tup)) for tup in data)
    if isinstance(ds.measure, Record) and len(ds.shape) == 0:
        names = ds.measure.names
        return dict(zip(names, data))
    if not isinstance(ds.measure, Record):
        return data
    raise NotImplementedError()


@contextmanager
def ignoring(*exceptions):
    try:
        yield
    except exceptions:
        pass


from multipledispatch import Dispatcher
sample = Dispatcher('sample')
