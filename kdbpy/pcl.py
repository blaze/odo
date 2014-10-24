""" pcl top-level managemenet """
import logging
from kdbpy import kdb, examples as exmpl

class PCL(object):
    examples = exmpl.Examples()

    def __init__(self, credentials=None, q_exec=None, start='restart'):
        """
        Parameters
        ----------
        credentials: credentials if specified, default None (use default credentials)
        q_exec: q_exec path (default of None uses the default path)
        start : boolean, default False
           if True and process is running, return
           if 'restart' and process is running, restart it
           if False raise ValueError if the process is running
        """

        self.kq = kdb.KQ(credentials=credentials, path=q_exec)

        if start:
            self.start(start=start)

    # context manager, so allow
    # with PCL() as p:
    #    pass
    def __enter__(self):
        # don't restart if already started
        self.start(start=True)
        return self

    def __exit__(self, *args):
        self.stop()
        return True

    def __str__(self):
        """ return a string representation of the connection """
        return "[{0}: {1}]".format(type(self).__name__,self.kq)

    __repr__ = __str__

    # start stop the kdb client/server
    def start(self, start='restart'):
        """ start up kdb/q process and connect server """
        self.kq.start()

    def stop(self):
        """ terminate kdb/q process and connecting server """
        self.kq.stop()

    @property
    def is_started(self):
        """ return boolean if kdb is started """
        return self.kq.is_started

    def eval(self, *args, **kwargs):
        """ send the evaluation expression and options to the compute engine """
        if not self.is_started:
            raise ValueError("kdb/q is not started!")
        return self.kq.kdb.eval(*args, **kwargs)
