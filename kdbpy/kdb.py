""" kdb process management """
import os
import time
import subprocess
import atexit

from collections import namedtuple

import pandas as pd
from qpython import qconnection

# credentials
Credentials = namedtuple('Credentials', ['host', 'port', 'username',
                                         'password'])
default_credentials = Credentials('localhost',5001,'user','password')

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
    return Credentials(host=host,
                       port=port,
                       username=username,
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
        return str(self.kdb)

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

class Q(object):
    """ manage the q exec process """
    _object = None

    @classmethod
    def create(cls, credentials=None, path=None):
        """ enforce this as a singleton object """
        if cls._object is None:
            o = cls._object = cls(credentials=credentials, path=path)
        else:
            o = cls._object

            # we must have the same credentials!
            o.check(credentials=credentials, path=path)

        return o

    def __init__(self, credentials=None, path=None):
        self.credentials = credentials
        self.path = self.get_executable(path)
        self.process = None

    def __str__(self):
        """ return a string representation of the connection """
        if self.process is not None:
            s = "{0}:{1}".format(self.path,self.pid)
        else:
            s = "{0}:not started".format(self.path)
        return s

    __repr__ = __str__

    @property
    def pid(self):
        try:
            return self.process.pid
        except:
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
                import platform
                arch_name = platform.system()
                archd = {
                    'Darwin' : ['m32','q'],
                    'linux' : ['l32','q'],
                    'nt' : ['w32','q.exe'],
                    }
                arch = archd[arch_name]
                import kdbpy
                path = os.path.join(*[os.path.dirname(os.path.abspath(kdbpy.__path__[0])),'bin'] + arch)
            except (Exception) as e:
                raise RuntimeError("cannot locate q executable: {0}".format(e))
        return path

    @property
    def is_started(self):
        """ check if the q process is actually running """
        if self.process is not None:
            return True

        #### TODO # use psutil here!
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

        # launche the subprocess, redirecting stdout/err to devnull
        # alternatively we can redirect to a PIPE and use .communicate()
        # that can potentially block though
        with open(os.devnull, 'w') as devnull:
            try:
                self.process = subprocess.Popen([self.path , '-p', str(self.credentials.port)], stdin=devnull, stdout=devnull, stderr=devnull)
            except (Exception) as e:
                raise ValueError("cannot start the q process: {0} [{1}]".format(self.path,e))

        # register our exit function
        atexit.register(lambda : self.stop())

        #### TODO - need to wait for the process to start here
        #### maybe communicate and wait till it starts before returning
        time.sleep(0.25)

        return self

    def stop(self):
        """ terminate the q_process, returning boolean if it existed previously """
        if self.process is not None:
            try:
                self.process.terminate()
                return True
            except:
                pass
            finally:
                self.process = None
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
            s = 'client/server not started'

        return "{0}: [{1}]".format(type(self).__name__,s)

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

    def eval(self, expr, *args):
        """
        Parameters
        ----------
        expr: a string q expression
        args: a list of positional parameters to pass into the q expression

        Returns
        -------
        a scalar, a list, or a numpy 1-d array or a DataFrame

        """
        return self.q.sync(expr)
