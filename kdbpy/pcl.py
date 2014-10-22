""" pcl top-level managemenet """
import logging
from kdbpy import kdb, examples as exmpl

class PCL(object):
    examples = exmpl.Examples()

    def __init__(self, credentials_kdb=None, q_exec=None, start_kdb='restart'):
        """
        Parameters
        ----------
        credentials_kdb: credentials if specified, default None (use default credentials)
        q_exec: q_exec path (default of None uses the default path)
        start_kdb: boolean/'restart'
            start kdb upon creation; if 'restart' is specified then
            restart if needed
        """

        self.kdb = None

        if start_kdb:
            self.start_kdb(credentials=credentials_kdb, path=q_exec, restart=start_kdb)

    # context manager, so allow
    # with PCL() as p:
    #    pass
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.stop()
        return True

    def __str__(self):
        """ return a string representation of the connection """
        if self.kdb is not None:
            kdb = str(self.kdb)
        else:
            kdb = 'kdb client/server not started'

        return "{0}: [{1}]".format(type(self).__name__,kdb)

    __repr__ = __str__

    # start stop the kdb client/server
    def start_kdb(self, credentials=None, path=None, restart='restart'):
        """ start up kdb/q process and connect server """
        self.kdb = kdb.launch_kdb(credentials=credentials, path=path, parent=self, restart=restart)

    def stop_kdb(self):
        """ terminate kdb/q process and connecting server """
        if self.kdb is not None:
            self.kdb.stop()
            self.kdb = None
        kdb.q_stop_process()

    @property
    def is_kdb(self):
        """ return boolean if kdb is started """
        return self.kdb is not None and self.kdb.is_initialized

    def stop(self):
        """ all stop """
        self.stop_kdb()

    def eval(self, *args, **kwargs):
        """ send the evaluation expression and options to the compute engine """
        return self.kdb.eval(*args, **kwargs)
