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

try:
    q_executable = os.environ['QEXEC']
except KeyError:
    raise KeyError('Please set the QEXEC environment variable to point to a Q'
                   ' executable')
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

# q process
def q_start_process(cred, restart=False):
    """
    create the q executable process, returning the handle

    Parameters
    ----------
    cred : a Credentials
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
        q_handle = subprocess.Popen([ q_executable, '-p', str(cred.port)], stdin=devnull, stdout=devnull, stderr=devnull)

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

def launch_kdb(credentials=None):
    """
    Parameters
    ----------
    credentials: Credentials, or default to kdb.credentials()

    Returns
    -------
    a KDB object, with a started q engine
    """

    if credentials is None:
        credentials = get_credentials()
    q_start_process(credentials, restart=False)
    return KDB(parent=None, credentials=credential).start()

class KDB(object):
    """ represents the interface to qPython object """

    def __init__(self, parent, credentials):
        """ Hold the parent reference, and connection credentials """
        self.parent = parent
        self.credentials = credentials
        self.q = None

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

class Example(object):
    """ hold an example record, including the q string and the expected result """

    def __init__(self, key, q, result=None):
        self.key = key
        self.q = q
        self.result = result


class Examples(object):
    """ hold q examples for interactive use, serves up a dict-like interface """

    def __init__(self):
        self.data = self.create_data()

    def create_data(self):
        return [
            Example('table1',
                    '([]a:til 10;b:reverse til 10;c:`foo;d:{x#.Q.a}each til 10)',
                    pd.DataFrame({'a': range(0,10), 'b' : range(10,0,-1), 'c' : 'foo'})),
            Example('table2',
                    'flip `name`iq`fullname!(`Dent`Beeblebrox`Prefect;98 42 126;("Arthur Dent"; "Zaphod Beeblebrox"; "Ford Prefect"))',
                    pd.DataFrame()),
            Example('table3',
                    '([eid:1001 0N 1003;sym:`foo`bar`] pos:`d1`d2`d3;dates:(2001.01.01;2000.05.01;0Nd))',
                    pd.DataFrame()),
            Example('scalar1','42',42),
            Example('datetime','20130101 10:11:12'),
            ]

    @property
    def keys(self):
        return self.data.keys()

    @property
    def values(self):
        return self.data.values()

    def __getitem__(self, key):
        return self.data[key]

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(( (e.key, e) for e in self._data ))
