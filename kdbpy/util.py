import IPython
import os
import sqlalchemy as sa
import psutil

class PrettyMixin(object):

    def __repr__(self):
        return IPython.lib.pretty.pretty(self)

class CredsMixin(object):

    # credentials accessors

    @property
    def host(self):
        return self.credentials.host

    @property
    def port(self):
        return self.credentials.port

    @property
    def username(self):
        return self.credentials.username

    @property
    def password(self):
        return self.credentials.password

    @property
    def is_fixed_port(self):
        return self.credentials.is_fixed_port


def parse_connection_string(uri):
    from kdbpy.kdb import Credentials
    params = sa.engine.url.make_url(uri)
    return Credentials(username=params.username, password=params.password,
                       host=params.host, port=params.port)


def normpath(path):
    return path.replace(os.sep, '/')


hostname = 'localhost'


def find_running_process(process, port):
    """
    find an actual running process with our pid
    return None if no process found

    Paramaters
    ---------
    process : a psutil process
    port : a port for discovery

    """

    # only q processes with at least a single connection
    # leave everything else alone
    for proc in psutil.process_iter():
        try:
            name = proc.name()
        except psutil.AccessDenied:
            pass
        else:
            if name == 'q' or name == 'q.exe':
                try:
                    conns = proc.connections()
                except psutil.AccessDenied:
                    pass
                else:
                    for conn in conns:  # probably a single element list
                        _, p = conn.laddr
                        if p == port:
                            return proc

def kill_process(process):
    """
    kill a process and its children

    Parameters
    ----------
    process : a psutil process
    """

    def killp(proc):
        try:
            proc.terminate()
        except psutil.NoSuchProcess:
            pass

    # need to make sure that we kill any process children as well
    try:
        for proc in process.children():
            killp(proc)
    except psutil.NoSuchProcess:
        pass

    killp(process)
