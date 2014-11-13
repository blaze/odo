""" kdb process management """
from __future__ import print_function, absolute_import

import os
import socket
import atexit
import platform
import getpass
import subprocess

from itertools import chain

import psutil

import pandas as pd
import numpy as np
from qpython import qconnection, qtemporal

from toolz.compatibility import range

from blaze import Data, CSV
from kdbpy.util import normpath


class Credentials(object):
    """Lightweight credentials container.

    Parameters
    ----------
    host : str, optional
        Defaults to ``socket.gethostname()``
    port : int
        Defaults to 5000
    username : str
        Defaults to ``getpass.getuser()``
    password str or None
        Defaults to None
    """
    __slots__ = 'host', 'port', 'username', 'password'

    def __init__(self, host=None, port=None, username=None, password=None):
        super(Credentials, self).__init__()
        self.host = host if host is not None else socket.gethostname()
        self.port = port if port is not None else 5000
        self.username = username if username is not None else getpass.getuser()
        self.password = password if password is not None else ''

    def __hash__(self):
        return hash(tuple(getattr(self, slot) for slot in self.__slots__))

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__,
                           ', '.join('%s=%r' % (slot, getattr(self, slot))
                                     for slot in self.__slots__))


default_credentials = Credentials()


class BlazeGetter(object):
    def __init__(self, credentials):
        self.credentials = credentials

    def __getitem__(self, tablename):
        assert isinstance(tablename, basestring), 'tablename must be a string'
        creds = self.credentials
        template = 'kdb://{0.username}@{0.host}:{0.port}::{tablename}'
        return Data(template.format(creds, tablename=tablename))


# launch client & server
class KQ(object):
    """ manage the kdb & q process """

    def __init__(self, credentials=default_credentials, start=False, path=None):
        """
        Parameters
        ----------
        credentials: Credentials, or default to kdb.credentials()
        path: path to q_exec, default None (use arch default)
        start: boolean/'restart':
        how to to restart kdb if already started

        Returns
        -------
        a KDB and Q object, with a started q engine
        """
        self.credentials = credentials
        self.q = Q(credentials=credentials, path=path)
        self.kdb = KDB(credentials=self.credentials)
        if start:
            self.start(start=start)
        self._data = BlazeGetter(self.credentials)
        self._loaded = set()

    def __repr__(self):
        return '{0.__class__.__name__}(kdb={0.kdb}, q={0.q})'.format(self)

    # context manager, so allow
    # with KQ() as kq:
    #    pass
    def __enter__(self):
        # don't restart if already started
        self.start(start=True)
        return self

    def __exit__(self, *args):
        self.stop()
        return True

    @property
    def is_started(self):
        return self.q.is_started and self.kdb.is_started

    def start(self, start='restart'):
        """ start all """
        self.q.start(start=start)
        self.kdb.start()
        return self

    def stop(self):
        """ stop all """
        self.kdb.stop()
        self.q.stop()
        return self

    def eval(self, *args, **kwargs):
        return self.kdb.eval(*args, **kwargs)

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
        csv = CSV(filename, encoding=encoding, *args, **kwargs)
        columns = csv.columns
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

    @property
    def tables(self):
        types = {True: 'partitioned', False: 'splayed', -1: 'binary'}
        names = self.eval(r'\a').tolist()
        code = r'{[x] {t: .Q.qp[x]; $[(type t) = -7h; -1; t]}[eval x]} each value "\\a"'
        parted = pd.Series(self.eval(code))
        values = pd.Series(types)[parted].values
        return pd.DataFrame({'name': names, 'kind': values})[['name', 'kind']]

    @property
    def memory(self):
        result = self.eval('.Q.w[]')
        return pd.Series(result.values, index=result.keys, name='memory')

    @property
    def data(self):
        return self._data


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args,
                                                                 **kwargs)
        inst = cls._instances[cls]
        creds = list(filter(lambda x: isinstance(x, Credentials),
                            chain(args, kwargs.values())))
        try:
            creds = creds[0]
        except IndexError:
            creds = None  # passed no args
        if creds is not None and inst.credentials != creds:
            raise ValueError('Different credentials: %s than existing process: '
                             '%s' % (creds, inst.credentials))
        return inst


class Q(object):
    """ manage the q exec process """
    __metaclass__ = Singleton
    __slots__ = 'credentials', 'path', 'process'

    def __init__(self, credentials, path=None):
        self.credentials = credentials
        self.path = self.get_executable(path)
        self.process = None

    def __repr__(self):
        """return a string representation of the connection"""
        return "{0.__class__.__name__}(path={0.path}, pid={0.pid})".format(self)

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

    def find_running_process(self):
        """
        find an actual running process with our pid
        return None if no process found

        """
        if self.process is not None:
            return self.process

        # only q processes with at least a single connection
        # leave everything else alone
        for proc in psutil.process_iter():
            try:
                name = proc.name()
            except psutil.AccessDenied:
                pass
            else:
                if name == 'q' or name == 'q.exe':
                    conns = proc.connections()
                    for conn in conns:  # probably a single element list
                        _, port = conn.laddr
                        if port == self.credentials.port:
                            return proc

    @property
    def is_started(self):
        """
        check if the q process is actually running

        if it IS running, then set the process variable
        """

        self.process = self.find_running_process()
        return self.process is not None

    def start(self, start=True):
        """
        create the q executable process, returning the handle

        Parameters
        ----------
        start : boolean, default False
           if True and process is running, return
           if 'restart' and process is running, restart it
           if False raise ValueError if the process is running

        Returns
        -------
        self

        """

        # already started and no restart specified
        if start is True:
            if self.process is not None:
                return self

        # restart the process if needed
        elif start == 'restart':
            pass

        # raise if the process is actually running
        # we don't want to have duplicate processes
        else:
            if self.is_started:
                raise ValueError("q process already running!")

        self.stop()

        # launch the subprocess, redirecting stdout/err to devnull
        # alternatively we can redirect to a PIPE and use .communicate()
        # that can potentially block though
        with open(os.devnull, 'w') as wnull, open(os.devnull, 'r') as rnull:
            self.process = psutil.Popen([self.path, '-p',
                                         str(self.credentials.port)],
                                        stdin=rnull, stdout=wnull,
                                        stderr=subprocess.STDOUT)

        # register our exit function
        atexit.register(self.stop)
        return self

    def stop(self):
        """ terminate the q_process, returning boolean if it existed previously
        """
        self.process = self.find_running_process()
        if self.process is not None:
            try:
                self.process.terminate()
            except psutil.NoSuchProcess:  # we've already been killed
                pass
            del self.process
            self.process = None
            return True
        return False


class KDB(object):
    """ represents the interface to qPython object """

    def __init__(self, credentials):
        """ Hold the connection credentials """
        self.credentials = credentials
        self.q = None

    def __str__(self):
        """ return a string representation of the connection """
        if self.q is not None:
            s = "{0} -> connected".format(str(self.credentials))
        else:
            s = 'q client not started'

        return "[{0}: {1}]".format(type(self).__name__, s)

    __repr__ = __str__

    def start(self, ntries=1000):
        """ given credentials, start the connection to the server """
        cred = self.credentials
        self.q = qconnection.QConnection(host=cred.host,
                                         port=cred.port,
                                         username=cred.username,
                                         password=cred.password,
                                         pandas=True)
        e = None
        for i in range(ntries):
            try:
                self.q.open()
            except socket.error as e:
                pass
            else:
                break
        else:
            raise ValueError("Unable to connect to Q server after %d tries: %s"
                             % (ntries, e))
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


def which(exe):
    path = os.environ['PATH']
    for p in path.split(os.pathsep):

        # windows has things on the path that may not be directories so we need
        # to check
        if os.path.isdir(p):
            for f in map(os.path.basename, os.listdir(p)):
                if f == exe:
                    return os.path.join(p, f)
    raise OSError("Cannot find %r on path %s" % (exe, path))


# TEMPORALS
_q_base_timestamp = pd.Timestamp('2000-01-01')
_q_base_np_datetime = np.datetime64('2000-01-01 00:00:00')
