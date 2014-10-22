""" pcl top-level managemenet """
import logging
from kdbpy import kdb

class PCL(object):
    examples = kdb.Examples()

    def __init__(self, start_kdb='restart'):
        self.kdb = None

        if start_kdb:
            self.start_kdb(start_kdb)

    # context manager, so allow
    # with PCL() as p:
    #    pass
    def __enter__(self):
        return self

    def __exit__(self):
        self.stop()
        return self

    def __str__(self):
        """ return a string representation of the connection """
        if self.kdb is not None:
            kdb = str(self.kdb)
        else:
            kdb = 'kdb client/server not started'

        return "{0}: [{1}]".format(type(self).__name__,kdb)

    __repr__ = __str__

    # start stop the kdb client/server
    def start_kdb(self, how='restart'):
        """ start up kdb/q process and connect server """
        cred = kdb.get_credentials()
        kdb.q_start_process(cred, restart=how)
        self.kdb = kdb.KDB(parent=self, credentials=cred).start()

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
