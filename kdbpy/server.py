""" kdb process management """
import os
import subprocess
import atexit
import logging

from collections import namedtuple

# constants
q_default_host = 'localhost'
q_default_port = 5001
q_default_username = 'user'
q_default_password = 'password'

### TODO - need arch agnostic method of finding this
q_executable = '/Users/jreback/q/m32/q'
q_handle = None

# credentials
Credentials = namedtuple('Credentials',['host','port','username','password'])
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

    q_kill_process()

    # launche the subprocess, redirecting stdout/err to devnull
    # alternatively we can redirect to a PIPE and use .communicate()
    # that can potentially block though
    with open(os.devnull, 'w') as devnull:
        q_handle = subprocess.Popen([ q_executable, '-p', str(cred.port)], stdin=devnull, stdout=devnull, stderr=devnull)
    return q_handle

@atexit.register
def q_kill_process():
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
