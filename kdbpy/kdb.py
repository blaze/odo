""" kdb process management """
from __future__ import print_function, absolute_import

import os
import socket
import atexit
import platform
import getpass
import subprocess
import pprint
import random

from itertools import chain, count

import psutil

import pandas as pd
import numpy as np
from qpython import qconnection, qtemporal

from toolz.compatibility import range

import blaze as bz
import kdbpy
from kdbpy.util import (normpath, hostname, PrettyMixin, CredsMixin,
                        find_running_process, kill_process, which)

class PortInUse(ValueError): pass
class PortIsFixed(ValueError): pass


# out starting port number
DEFAULT_START_PORT_NUMBER = 47823
port_iter = count(DEFAULT_START_PORT_NUMBER)

class Credentials(PrettyMixin):
    """Lightweight credentials container.

    Parameters
    ----------
    host : str, optional
        Defaults to ``'localhost'``
    port : int
        Defaults to a port of %s
    username : str
        Defaults to ``getpass.getuser()``
    password str or None
        Defaults to None
    """ % str(DEFAULT_START_PORT_NUMBER)
    __slots__ = 'host', 'port', 'username', 'password', 'is_fixed_port'

    def __init__(self, host=None, port=None, username=None, password=None):
        super(Credentials, self).__init__()
        self.host = host if host is not None else hostname
        self.port = port
        self.username = username if username is not None else getpass.getuser()
        self.password = password if password is not None else ''

        self.is_fixed_port = port is not None

        # create assignable ports
        if not self.is_fixed_port:
            self.port = next(port_iter)

    def get_port(self):
        """
        set the port to random port in the port assigned range
        If we are a fixed port, this will raise PortIsFixed
        """
        if self.is_fixed_port:
            raise PortIsFixed

        self.port = next(port_iter)
        yield self.port

    def __copy__(self):
        """ copy-constructor, duplicate the iterator """
        new_self = type(self)()
        new_self.__dict__.update(self.__dict__)
        if not self.is_fixed_port:
            new_self.port = count(self.port)
        return new_self

    def __hash__(self):
        return hash(tuple(getattr(self, slot) for slot in self.__slots__))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def _repr_pretty_(self, p, cycle):
        assert not cycle, 'cycles not allowed'
        name = type(self).__name__
        fields = self.__slots__
        n = len(fields)
        with p.group(len(name) + 1, '%s(' % name, ')'):
            for i, slot in enumerate(fields, start=1):
                p.text('%s=%r' % (slot, getattr(self, slot)))
                if i < n:
                    p.text(',')
                    p.breakable()


# launch client & server
class KQ(PrettyMixin, CredsMixin):
    """ manage the kdb & q process """

    def __init__(self, credentials=None, start=False, path=None, verbose=False, cache=True):
        """
        Parameters
        ----------
        credentials: Credentials, or default to kdb.credentials()
        path: path to q_exec, default None (use arch default)
        start: boolean/'restart':
          how to to restart kdb if already started
        verbose: boolean, turn on verbose mode
        cache: use caching mode for tables/data

        Returns
        -------
        a KDB and Q object, with a started q engine
        """

        self.verbose = verbose

        if credentials is None:
            credentials = Credentials()
        self.credentials = credentials
        self.q = Q(credentials=credentials, path=path)
        self.kdb = KDB(credentials=credentials)
        self._loaded = set()

        self.cache = cache
        self.reset_cache()

        if start:
            self.start(start=start)
            self.load_libs()

    def load_libs(self,
                  libpath=os.path.join(os.path.dirname(kdbpy.__file__), 'q'),
                  libs=('lib.q',)):
        for lib in libs:
            path = os.path.join(libpath, lib)
            if not os.path.exists(path):
                raise OSError('Non-existent file %r' % path)
            self.read_kdb(normpath(path))

    def _repr_pretty_(self, p, cycle):
        assert not cycle, 'cycles not allowed'
        name = type(self).__name__
        start = '%s(' % name
        p.text('connected: %s\n' % (getattr(self.kdb.q, '_connection', None)
                                    is not None))
        with p.group(len(start), start, ')'):
            p.text('kdb=')
            p.pretty(self.kdb)
            p.text(',')
            p.breakable()
            p.text('q=')
            p.pretty(self.q)
            p.breakable()
            p.text('ncached=')
            p.pretty(self.ncached()),
            p.breakable()
            p.text('cached=')
            p.pretty(self._cache.keys())
        return '{0.__class__.__name__}(kdb={0.kdb}, q={0.q})'.format(self)

    # context manager, so allow
    # with KQ() as kq:
    #    pass
    def __enter__(self):
        # don't restart if already started
        if not self.is_started:
            self.start(start=True)
        return self

    def __exit__(self, *args):
        self.stop()
        return True

    @property
    def is_started(self):
        return self.q.is_started and self.kdb.is_started

    def start(self, start='restart'):
        """
        starting may involve a port collision with an existing process

        if we have a collision, then get the next port and update the credentials
        and try to start again

        """

        while True:

            try:

                self.q.start(start=start)

            except PortInUse:

                if self.is_fixed_port:
                    raise PortIsFixed("port {port} is in use".format(port=self.port))

                # in use, so get a new port
                next(self.credentials.get_port())

            except StopIteration:
                # no port left
                raise ValueError("exhausted all designated ports")

            else:
                break

        self.kdb.start()
        return self

    def stop(self):
        """ stop all """
        self.kdb.stop()
        self.q.stop()
        return self

    def eval(self, *args, **kwargs):

        # print the query if verbose is set
        if self.verbose:
            pprint.pprint((args, kwargs))

        # execute the query
        result = self.kdb.eval(*args, **kwargs)

        # we set a variable, flush the cache
        if result is None:
            self.reset_cache()

        return result

    def read_csv(self, filename, table, encoding=None, *args, **kwargs):
        """Put a CSV file's data into the Q namespace

        Parameters
        ----------
        filename : str
            The name of the CSV file to load
        table : str
            The name of the variable to construct in Q space
        sep : str
            The separator to pass to Q
        dshape : datashape
            The names and types of the columns

        Examples
        --------
        >>> from blaze import discover
        >>> from pandas import DataFrame
        >>> from pandas.util.testing import ensure_clean
        >>> df = DataFrame({'price': [1, 2, 3],
        ...                 'sym': list('abc')}).sort_index(axis=1)
        >>> dshape = discover(df)
        >>> with KQ(start=True) as kq:
        ...     with ensure_clean('temp.csv') as f:
        ...         df.to_csv(f, index=False)
        ...         n = kq.read_csv(f, table='trade', dshape=dshape)
        ...     kq.eval('trade')
           price sym
        0      1   a
        1      2   b
        2      3   c

        With option types (the extra whitespace in the repr is necessary)

        >>> import numpy as np
        >>> df = DataFrame({'price': [1, 2, np.nan],
        ...                 'sym': list('abc'),
        ...                 'conn': list('AB') + [np.nan]})[['price', 'sym',
        ...                                                  'conn']]
        >>> with KQ(start=True) as kq:
        ...     with ensure_clean('temp.csv') as f:
        ...         df.to_csv(f, index=False)
        ...         kq.read_csv(f, table='trade')
        ...     kq.eval('trade')
           price sym conn
        0      1   a    A
        1      2   b    B
        2    NaN   c  NaN
        """
        csv = bz.CSV(filename, encoding=encoding, *args, **kwargs)
        dshape = bz.discover(csv)
        columns = dshape.measure.names
        params = dict(table=table,
                      columns='; '.join('`$"%s"' % column for column in
                                        columns),
                      filename=normpath(filename))

        # load up the Q CSV reader
        self.read_kdb(os.path.join(os.path.dirname(__file__), 'q',
                                   'csvutil.q'))
        s = ('{table}: ({columns}) xcol .csv.read[`$":{filename}"]'
             ''.format(**params))
        self.eval(s)

    def set(self, key, value):
        """Set a variable `key` in kdb with the object `value`.

        Parameters
        ----------
        key : str
            The name of the variable to set
        value : object
            the value to set

        """
        # flush the cache
        self.reset_cache()

        self.kdb.q('set', np.string_(key), value)

    def get(self, key):
        """Return the variable `key` from kdb.

        Parameters
        ----------
        key : str
            The name of the variable to get

        Returns
        -------
        obj : Python object
        """
        return self.kdb.eval(str(key))

    def read_kdb(self, filename):
        """Load a binary file in KDB format

        Parameters
        ----------
        filename : str
            The name of the kdb file to load. Must be a valid Q identifier

        Returns
        -------
        name : str
            The name of the table loaded

        Examples
        --------
        >>> import os
        >>> from kdbpy.exampleutils import example_data
        >>> datapath = example_data('t')
        >>> kq = KQ(start=True)
        >>> tablepath = kq.read_kdb(datapath)
        >>> kq.eval('`id in cols t')
        True
        >>> kq.eval('.Q.qt t')
        True
        >>> int(kq.eval('.Q.qp t'))
        0
        """
        filename = normpath(os.path.abspath(filename))
        if filename not in self._loaded:
            result = self.eval(r'\l %s' % filename)
            self._loaded.add(filename)
        else:
            result = None
        return result

    ### cache interaction ###
    def ncached(self):
        """ return the number of cached tables """
        t = self._cache.get('tables')
        if t is not None:
            return len(t)
        return 0

    def reset_cache(self):
        self._cache = dict()

    def retrieve(self, key, f):
        """
        retrieve an item from the cache by key, if its not present or
        the cache is not enabled, execute and cache with f
        """
        if self.cache:
            res = self._cache.get(key)
            if res is None:
                res = f()
                self._cache[key] = res
        else:
            res = f()
        return res

    @property
    def tables(self):
        return self.retrieve('tables',self._tables)

    def _tables(self):
        """ return all of the table meta data """
        types = {True: 'partitioned', False: 'splayed', -1: 'binary'}
        names = self.eval(r'\a').tolist()
        code = r'{[x] {t: .Q.qp[x]; $[(type t) = -7h; -1; t]}[eval x]} each value "\\a"'
        values = [types[p] for p in self.eval(code)]
        return pd.DataFrame({'name': names, 'kind': values})[['name', 'kind']]

    @property
    def dshape(self):
        return self.retrieve('dshape',self._dshape)

    def _dshape(self):
        """ return datashape of the KQ object """
        return bz.discover(self)

    @property
    def memory(self):
        result = self.eval('.Q.w[]')
        return pd.Series(result.values, index=result.keys, name='memory')

    def __getitem__(self, key):
        assert isinstance(key, basestring), 'key must be a string'
        if key in set(self.tables.name):

            dshape = self.retrieve('dshape',self._dshape)
            return bz.Data(self,dshape=dshape)[key]

        return self.get(key)

    def __setitem__(self, key, value):
        # flush the cache
        self.reset_cache()

        # set
        self.set(key, value)

class Q(PrettyMixin, CredsMixin):
    """ manage the q exec process """
    __slots__ = 'credentials', 'path', 'process'
    processes = {}

    def __init__(self, credentials, path=None):
        self.credentials = credentials
        self.path = self.get_executable(path)

    @property
    def process(self):
        """ return my process handle """
        return self.processes.get(self.credentials)

    def _repr_pretty_(self, p, cycle):
        """return a string representation of the connection"""
        name = type(self).__name__
        start = '%s(' % name
        with p.group(len(start), start, ')'):
            p.text('path=')
            p.pretty(self.path)
            p.text(',')
            p.breakable()
            p.text('pid=')
            p.pretty(self.pid)

    @property
    def pid(self):
        try:
            return self.process.pid
        except AttributeError:
            return None

    def get_executable(self, path=None):
        """
        get the path to the q executbale

        default file must be on user path
        """

        if path is None:
            arch_name = platform.system().lower()

            # the .bat assumes we have the q conda package installed
            archd = {'darwin': 'q', 'linux': 'q', 'windows': 'q.bat'}
            try:
                return which(archd[arch_name])
            except KeyError:
                raise OSError("Unsupported operating system: %r" % arch_name)
        return path

    @property
    def is_started(self):
        """
        check if the q process is actually running
        """

        return self.process is not None

    def start(self, start=True):
        """
        create the q executable process, returning the handle

        Parameters
        ----------
        start : boolean or string, default False
           if True and process is running, return
           if 'restart' and process is running, restart it
           if False raise PortInUse if the process is running

        Raises
        ------
        PortInUse : if the credentials port is already taken
           and we are not that process

        Returns
        -------
        self

        """

        proc = self.process or find_running_process(self.process, self.port)

        # already started and no restart specified
        if start is True:
            if proc is not None:
                return self

        # restart the process if needed
        elif start == 'restart':

            self.stop()
            proc = None

        # if we find a running process that matches our port
        # then raise. We cannot connect to an existing process
        if proc is not None:
            raise PortInUse("cannot create a Q process for attaching " \
                            "to the port {port}".format(port=self.port))

        # launch the subprocess, redirecting stdout/err to devnull
        # alternatively we can redirect to a PIPE and use .communicate()
        # that can potentially block though
        with open(os.devnull, 'w') as wnull, open(os.devnull, 'r') as rnull:

            self.processes[self.credentials] = psutil.Popen([self.path, '-p',
                                                             str(self.port)],
                                                            stdin=rnull, stdout=wnull,
                                                            stderr=subprocess.STDOUT)


        # register our exit function
        atexit.register(self.stop)
        return self

    def stop(self):
        """ terminate the q_process, returning boolean if it existed previously
        """
        process = self.process or find_running_process(self.process, self.port)
        if process is not None:
            kill_process(process)

            # if we are actually killing a running processes
            # then this might not be in our processes map
            try:
                del self.processes[self.credentials]
            except KeyError:
                pass

            return True
        return False


class KDB(PrettyMixin):
    """ represents the interface to qPython object """

    def __init__(self, credentials):
        """ Hold the connection credentials """
        self.credentials = credentials
        self.q = None

    def _repr_pretty_(self, p, cycle):
        """ return a string representation of the connection """
        assert not cycle, 'cycles not allowed'
        name = type(self).__name__
        with p.group(len(name) + 1, '%s(' % name, ')'):
            p.pretty(self.credentials)
            if self.q is not None:
                p.text(',')
                p.breakable()
                p.text('q=QConnection(...)')

    def start(self, ntries=1000):
        """ given credentials, start the connection to the server """
        cred = self.credentials
        self.q = qconnection.QConnection(host=cred.host,
                                         port=cred.port,
                                         username=cred.username,
                                         password=cred.password,
                                         pandas=True)
        assert self.q._connection is None
        e = None
        for i in range(ntries):
            try:
                self.q.open()
            except socket.error as e:
                self.q.close()
            else:
                assert hasattr(self.q, '_writer')
                break
        else:
            raise ValueError("Unable to connect to Q server after %d tries: %s"
                             % (ntries, e))
        assert self.q._connection is not None
        return self

    def stop(self):
        """ stop the kdb client process """
        if self.q is not None:
            self.q.close()
            self.q = None
        return self

    @property
    def is_started(self):
        return self.q is not None

    def eval(self, expr, *args, **kwargs):
        """
        Parameters
        ----------
        expr: a string q expression or callable
        args: a list of positional parameters to pass into the q expression

        Returns
        -------
        a scalar, a list, or a numpy 1-d array or a DataFrame

        """

        if callable(expr):
            if len(args) or len(kwargs):
                result = expr(*args, **kwargs)
            else:
                result = expr()
        else:

            result = self.q.sync(expr, *args)

        # need to coerce datetime-like scalars
        if isinstance(result, qtemporal.QTemporal):

            result = result.raw
            if isinstance(result, np.datetime64):
                result = pd.Timestamp(result)
            elif isinstance(result, np.timedelta64):
                result = pd.Timedelta(result)

        return result

# TEMPORALS
_q_base_timestamp = pd.Timestamp('2000-01-01')
_q_base_np_datetime = np.datetime64('2000-01-01 00:00:00')
