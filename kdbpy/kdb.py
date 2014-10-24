""" kdb process management """
import os
import time
import atexit
import platform
import getpass
from collections import namedtuple

import psutil
import subprocess

import pandas as pd
import numpy as np
from qpython import qconnection, qtemporal
from pandas.core import common as com

import kdbpy

# credentials
Credentials = namedtuple('Credentials', ['host', 'port', 'username',
                                         'password'])

default_credentials = Credentials('localhost', 5001, getpass.getuser(), None)


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
        self.q = Q.create(credentials=credentials, path=path)
        self.kdb = KDB(credentials=self.credentials)
        if start is not None:
            self.start(start=start)

    def __str__(self):
        return "{0} - {1}".format(self.kdb,self.q)

    __repr__ = __str__

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


class Q(object):
    """ manage the q exec process """
    _object = None

    @classmethod
    def create(cls, credentials=None, path=None):
        """ enforce this as a singleton object """
        if cls._object is None:
            if credentials is None:
                credentials = get_credentials()
            o = cls._object = cls(credentials=credentials, path=path)
        else:
            o = cls._object

            # we must have the same credentials!
            o.check(credentials=credentials, path=path)

        return o

    def __init__(self, credentials, path=None):
        self.credentials = credentials
        self.path = self.get_executable(path)
        self.process = None

    def __str__(self):
        """ return a string representation of the connection """
        if self.process is not None:
            s = "{0} -> {1}".format(self.path,self.pid)
        else:
            s = "q client not started"


        return "[{0}: {1}]".format(type(self).__name__,s)

    __repr__ = __str__

    @property
    def pid(self):
        try:
            return self.process.pid
        except AttributeError:
            return None

    def check(self, credentials, path):
        if path is not None and path != self.path:
            raise ValueError("incompatible path with existing process")
        if credentials is not None and credentials != self.credentials:
            raise ValueError("incompatible credentials with existing process")

    def get_executable(self, path=None):
        """
        get the path to the q executbale

        this is fixed for various architectures
        """

        if path is None:
            try:
                arch_name = platform.system()
                archd = {
                    'Darwin': ['m32', 'q'],
                    'linux': ['l32', 'q'],
                    'nt': ['w32', 'q.exe'],
                }
                arch = archd[arch_name]
                path = os.path.join(*[os.path.dirname(os.path.abspath(kdbpy.__path__[0])),'bin'] + arch)
            except (Exception) as e:
                raise RuntimeError("cannot locate q executable: {0}".format(e))
        return path

    def find_running_process(self):
        """
        find an actual running process with our pid
        return None if no process found

        """
        if self.process is not None:
            return self.process
        for proc in psutil.process_iter():
            try:
                name = proc.name()
                if 'q' in name:
                    cmdline = set(proc.cmdline())
                    if '-p' in cmdline and str(self.credentials.port) in cmdline:
                        return proc

            except (Exception) as e:
                # ignore these; we are only interesetd in user procs
                pass

        return None

    @property
    def is_started(self):
        """
        check if the q process is actually running

        if it IS running, then set the process variable
        """

        self.process = self.find_running_process()
        if self.process is not None:
            return True

        return False

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
            try:
                self.process = psutil.Popen([self.path , '-p',
                                             str(self.credentials.port)],
                                            stdin=rnull, stdout=wnull,
                                            stderr=subprocess.STDOUT)
            except Exception as e:
                raise ValueError("cannot start the q process: {0} [{1}]".format(self.path, e))

        # register our exit function
        atexit.register(self.stop)

        #### TODO - need to wait for the process to start here
        #### maybe communicate and wait till it starts before returning
        time.sleep(0.025)

        return self

    def stop(self):
        """ terminate the q_process, returning boolean if it existed previously
        """
        self.process = self.find_running_process()
        if self.process is not None:
            self.process.terminate()
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


    def start(self):
        """ given credentials, start the connection to the server """
        cred = self.credentials
        try:
            self.q = qconnection.QConnection(host=cred.host,
                                             port=cred.port,
                                             username=cred.username,
                                             password=cred.password,
                                             pandas=True)
            self.q.open()
        except (Exception) as e:
            raise ValueError("cannot connect to the kdb server: {0}".format(e))
        if not self.is_started:
            raise ValueError("kdb is not initialized: {0}".format(self.q))
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

                # these are created by adding an offset to np.datetime64('2000-01-01','ns')
                # which represents them in the local timezone; so since we want
                # timezone naive have to jump some hoops
                if result.dtype == 'M8[ms]':
                    result = _q_base_timestamp + (result.astype('M8[ns]') - _q_base_np_datetime)

                # this is M[D] dtype so ok
                else:
                    result = pd.Timestamp(result)

            elif isinstance(result, np.timedelta64):

                result = pd.Timedelta(result)

        return result

# TEMPORALS
_q_base_timestamp = pd.Timestamp('2000-01-01')
_q_base_np_datetime = np.datetime64('2000-01-01 00:00:00')
