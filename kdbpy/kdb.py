""" kdb process management """
import os
import time
import subprocess
import atexit

from collections import namedtuple

# pandas
import pandas as pd

# qPython
from qpython import qconnection

# constants
q_default_host = 'localhost'
q_default_port = 5001
q_default_username = 'user'
q_default_password = 'password'


def get_q_executable(path=None):
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

q_handle = None

# credentials
Credentials = namedtuple('Credentials', ['host', 'port', 'username',
                                         'password'])
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
        host = q_default_host
    if port is None:
        port = q_default_port
    if username is None:
        username = q_default_username
    if password is None:
        password = q_default_password
    return Credentials(host=host,
                       port=port,
                       username=username,
                       password=password)
# launch client & server
def launch_kdb(credentials=None, path=None, parent=None, restart='restart'):
    """
    Parameters
    ----------
    credentials: Credentials, or default to kdb.credentials()
    path: path to q_exec, default None (use arch default)
    parent: parent object
    restart: boolean/'restart':
        how to to restart kdb if already started

    Returns
    -------
    a KDB object, with a started q engine
    """

    if credentials is None:
        credentials = get_credentials()
    q_start_process(credentials=credentials, path=path, restart='restart')
    return KDB(parent=parent, credentials=credentials).start()



# q process
def q_start_process(credentials, path=None, restart=False):
    """
    create the q executable process, returning the handle

    Parameters
    ----------
    credentials : a Credentials
    path : the q executable path, defaults to a shipped 32-bit version
    restart : boolean, default False
       if True restart if the process is running
       if False raise ValueError if the process is running

    Returns
    -------
    a Popen process handle

    """
    global q_handle

    if not restart and q_handle is not None:
        raise ValueError("q process already running!")

    q_stop_process()

    # launche the subprocess, redirecting stdout/err to devnull
    # alternatively we can redirect to a PIPE and use .communicate()
    # that can potentially block though
    with open(os.devnull, 'w') as devnull:
        q_exec = get_q_executable(path)
        try:
            q_handle = subprocess.Popen([q_exec , '-p', str(credentials.port)], stdin=devnull, stdout=devnull, stderr=devnull)
        except (Exception) as e:
            raise ValueError("cannot start the q process: {0} [{1}]".format(q_exec,e))

    #### TODO - need to wait for the process to start here
    #### maybe communicate and wait till it starts before returning
    time.sleep(0.5)

    return q_handle


@atexit.register
def q_stop_process():
    """ terminate the q_process, returning boolean if it existed previously """
    global q_handle
    if q_handle is not None:
        try:
            q_handle.terminate()
            return True
        except:
            pass
        finally:
            q_handle = None
    return False


class KDB(object):
    """ represents the interface to qPython object """

    def __init__(self, parent, credentials):
        """ Hold the parent reference, and connection credentials """
        self.parent = parent
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
        if not self.is_initialized:
            raise ValueError("kdb is not initialized: {0}".format(self.q))
        return self

    def stop(self):
        """ stop the kdb client process """
        if self.q is not None:
            self.q.close()
            self.q = None
        return self

    @property
    def is_initialized(self):
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
