from __future__ import absolute_import, division, print_function

import warnings
import functools
import inspect
import datetime
import tempfile
import os
import shutil
import numpy as np

from contextlib import contextmanager
from multiprocessing.pool import ThreadPool

from multipledispatch import Dispatcher

from datashape import dshape, Record
from datashape.discovery import is_zero_time

from toolz import pluck, get, curry, keyfilter

from .compatibility import unicode

sample = Dispatcher('sample')


def iter_except(func, exception, first=None):
    """Call a `func` repeatedly until `exception` is raised. Optionally call
    `first` first.

    Parameters
    ----------
    func : callable
        Repeatedly call this until `exception` is raised.
    exception : Exception
        Stop calling `func` when this is raised.
    first : callable, optional, default ``None``
        Call this first if it isn't ``None``.

    Examples
    --------
    >>> x = {'a': 1, 'b': 2}
    >>> def iterate():
    ...     yield 'a'
    ...     yield 'b'
    ...     yield 'c'
    ...
    >>> keys = iterate()
    >>> diter = iter_except(lambda: x[next(keys)], KeyError)
    >>> list(diter)
    [1, 2]

    Notes
    -----
    * Taken from https://docs.python.org/2/library/itertools.html#recipes
    """
    try:
        if first is not None:
            yield first()
        while 1:  # True isn't a reserved word in Python 2.x
            yield func()
    except exception:
        pass


def ext(filename):
    _, e = os.path.splitext(filename)
    return e.lstrip(os.extsep)


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
def tmpfile(extension='', dir=None):
    extension = '.' + extension.lstrip('.')
    handle, filename = tempfile.mkstemp(extension, dir=dir)
    os.close(handle)
    os.remove(filename)

    try:
        yield filename
    finally:
        if os.path.exists(filename):
            if os.path.isdir(filename):
                shutil.rmtree(filename)
            else:
                try:
                    os.remove(filename)
                except OSError:
                    # sometimes we can't remove a generated temp file
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
def filetext(text, extension='', open=open, mode='w'):
    with tmpfile(extension=extension) as filename:
        f = open(filename, mode=mode)
        try:
            f.write(text)
        finally:
            try:
                f.close()
            except AttributeError:
                pass

        try:
            yield filename
        finally:
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                except OSError:
                    pass


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

    try:
        yield list(d)
    finally:
        for filename in d:
            if os.path.exists(filename):
                try:
                    os.remove(filename)
                except OSError:
                    pass


def normalize_to_date(dt):
    if isinstance(dt, datetime.datetime) and is_zero_time(dt.time()):
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
        return pluck(ds.measure.names, data, default=None)
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


def into_path(*path):
    """ Path to file in into directory

    >>> into_path('backends', 'tests', 'myfile.csv')  # doctest: +SKIP
    '/home/user/odo/odo/backends/tests/myfile.csv'
    """
    import odo
    return os.path.join(os.path.dirname(odo.__file__), *path)


@curry
def pmap(f, iterable):
    """Map `f` over `iterable` in parallel using a ``ThreadPool``.
    """
    p = ThreadPool()
    try:
        result = p.map(f, iterable)
    finally:
        p.terminate()
    return result


@curry
def write(triple, writer):
    """Write a file using the input from `gentemp` using `writer` and return
    its index and filename.

    Parameters
    ----------
    triple : tuple of int, str, str
        The first element is the index in the set of chunks of a file, the
        second element is the path to write to, the third element is the data
        to write.

    Returns
    -------
    i, filename : int, str
        File's index and filename. This is used to return the index and
        filename after splitting files.

    Notes
    -----
    This could be adapted to write to an already open handle, which would
    allow, e.g., multipart gzip uploads. Currently we open write a new file
    every time.
    """
    i, filename, data = triple
    with writer(filename, mode='wb') as f:
        f.write(data)
    return i, filename


def gentemp(it, suffix=None, start=0):
    """Yield an index, a temp file, and data for each element in `it`.

    Parameters
    ----------
    it : Iterable
    suffix : str or ``None``, optional
        Suffix to add to each temporary file's name
    start : int, optional
        A integer indicating where to start the numbering of chunks in `it`.
    """
    for i, data in enumerate(it, start=start):  # aws needs parts to start at 1
        with tmpfile('.into') as fn:
            yield i, fn, data


@curry
def split(filename, nbytes, suffix=None, writer=open, start=0):
    """Split a file into chunks of size `nbytes` with each filename containing
    a suffix specified by `suffix`. The file will be written with the ``write``
    method of an instance of `writer`.

    Parameters
    ----------
    filename : str
        The file to split
    nbytes : int
        Split `filename` into chunks of this size
    suffix : str, optional
    writer : callable, optional
        Callable object to use to write the chunks of `filename`
    """
    with open(filename, mode='rb') as f:
        byte_chunks = iter(curry(f.read, nbytes), '')
        return pmap(write(writer=writer),
                    gentemp(byte_chunks, suffix=suffix, start=start))


def filter_kwargs(f, kwargs):
    """Return a dict of valid kwargs for `f` from a subset of `kwargs`

    Examples
    --------
    >>> def f(a, b=1, c=2):
    ...     return a + b + c
    ...
    >>> raw_kwargs = dict(a=1, b=3, d=4)
    >>> f(**raw_kwargs)
    Traceback (most recent call last):
        ...
    TypeError: f() got an unexpected keyword argument 'd'
    >>> kwargs = filter_kwargs(f, raw_kwargs)
    >>> f(**kwargs)
    6
    """
    return keyfilter(keywords(f).__contains__, kwargs)


@curry
def copydoc(from_, to):
    """Copies the docstring from one function to another.

    Parameters
    ----------
    from_ : any
        The object to copy the docstring from.
    to : any
        The object to copy the docstring to.

    Returns
    -------
    to : any
        ``to`` with the docstring from ``from_``
    """
    to.__doc__ = from_.__doc__
    return to

def deprecated(replacement=None):
    """A decorator which can be used to mark functions as deprecated.
    replacement is a callable that will be called with the same args
    as the decorated function.
    """
    def outer(fun):
        msg = "{} is deprecated".format(fun.__name__)
        if replacement is not None:
            msg += "; use {} instead".format(replacement)
        if fun.__doc__ is None:
            fun.__doc__ = msg

        @functools.wraps(fun)
        def inner(*args, **kwargs):
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            return fun(*args, **kwargs)

        return inner
    return outer


def literal_compile(s):
    """Compile a sql expression with bind params inlined as literals.

    Parameters
    ----------
    s : Selectable
        The expression to compile.

    Returns
    -------
    cs : str
        An equivalent sql string.
    """
    return str(s.compile(compile_kwargs={'literal_binds': True}))
