""" kdb process management """
import os
# import time
import socket
import atexit
import platform
import getpass
import subprocess

# from future.builtins import range

from itertools import chain
from collections import namedtuple

import psutil

import pandas as pd
import numpy as np
from qpython import qconnection, qtemporal

from datashape.predicates import isrecord
import datashape as ds

# credentials
Credentials = namedtuple('Credentials', ['host', 'port', 'username',
                                         'password'])

default_credentials = Credentials('localhost', 5001, getpass.getuser(), None)


dstypes = {ds.bool_: 'b',
           ds.int8: 'x',
           ds.int16: 'h',
           ds.int32: 'i',
           ds.int64: 'j',
           ds.float32: 'e',
           ds.float64: 'f',
           ds.string: 's',
           ds.date_: 'd',
           ds.datetime_: 'p',
           ds.time_: 't'}


def dshape_to_qtypes(ds):
    assert isrecord(ds.measure), 'measure must be a record'
    return ds.measure.names, ''.join(dstypes[dstype].upper()
                                     for dstype in ds.measure.types)


def get_credentials(host=None, port=None, username=None, password=None):
    """
    Parameters
    ----------
    host : string or None
    port : string/int or None
    username : string or None
    password : string or None

    Returns
    -------
    a Credentials

    """

    if host is None:
        host = default_credentials.host
    if port is None:
        port = default_credentials.port
    if username is None:
        username = default_credentials.username
    if password is None:
        password = default_credentials.password
    return Credentials(host=host, port=port, username=username,
                       password=password)


# launch client & server
class KQ(object):
    """ manage the kdb & q process """

    def __init__(self, credentials=None, path=None, start=None):
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

        if credentials is None:
            credentials = get_credentials()
        self.credentials = credentials
        self.q = Q(credentials=credentials, path=path)
        self.kdb = KDB(credentials=self.credentials)
        if start is not None:
            self.start(start=start)

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

    def load_csv(self, filename, table, sep=',', dshape=None):
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

        Returns
        -------
        n : int
            The number of rows read in

        Examples
        --------
        >>> import tempfile
        >>> from blaze import discover
        >>> from pandas import DataFrame
        >>> from pandas.util.testing import ensure_clean
        >>> df = DataFrame({'price': [1, 2, 3],
        ...                 'sym': list('abc')}).sort_index(axis=1)
        >>> dshape = discover(df)
        >>> kq = KQ(get_credentials(), start='restart')
        >>> with ensure_clean('temp.csv') as f:
        ...     df.to_csv(f, index=False)
        ...     kq.load_csv(f, table='trade', dshape=dshape)
        >>> kq.eval('trade')
           price sym
        0      1   a
        1      2   b
        2      3   c
        >>> kq.stop() # doctest: +SKIP
        """
        s = '{table}: {columns} xcol (("{types}"; enlist "{sep}") 0:`$"{filename}")'
        columns, types = dshape_to_qtypes(dshape)
        params = dict(table=table,
                      columns=''.join('`%s' % column for column in columns),
                      types=types,
                      sep=sep,
                      filename=filename)
        return self.eval(s.format(**params))


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
            archd = {'darwin': 'q', 'linux': 'q', 'windows': 'q.exe'}
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
                if name == 'q':
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
            self.process = psutil.Popen([self.path , '-p',
                                         str(self.credentials.port)],
                                        stdin=rnull, stdout=wnull,
                                        stderr=subprocess.STDOUT)

        # register our exit function
        atexit.register(self.stop)

        #### TODO - need to wait for the process to start here
        #### maybe communicate and wait till it starts before returning
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

        return "[{0}: {1}]".format(type(self).__name__,s)

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
        for f in [x for x in os.listdir(p) if x not in ('..', '.')]:
            if os.path.basename(f) == exe:
                return os.path.join(p, f)
    raise OSError("Cannot find %r on path %s" % (exe, path))


# TEMPORALS
_q_base_timestamp = pd.Timestamp('2000-01-01')
_q_base_np_datetime = np.datetime64('2000-01-01 00:00:00')
