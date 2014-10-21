import unittest
import pytest

import numpy as np
from kdbpy import kdb, lib

# if you want to manually watch the server process
# set this to False
launch_server = True

manual_port = 5002

class Base(unittest.TestCase):

    def setUp(self):
        if launch_server:
            self.creds = kdb.get_credentials()
            kdb.q_start_process(self.creds)
        else:
            self.creds = kdb.get_credentials(port=manual_port)

    def tearDown(self):
        if launch_server:
            kdb.q_stop_process()

class Construction(Base):

    def test_construction(self):
        k = lib.KDB(self.creds).start()
        assert k.is_initialized
        k.stop()
        assert not k.is_initialized

        # require initilization
        with pytest.raises(ValueError):
            lib.KDB(kdb.get_credentials(port=0)).start()

class Eval(Base):

    def setUp(self):
        super(Eval, self).setUp()
        self.kdb = lib.KDB(self.creds).start()

    def tearDown(self):
        self.kdb.stop()
        super(Eval, self).tearDown()

    def test_evaluate_scalar(self):
        result = self.kdb.eval("42")
        assert result == 42

    def test_evaluate_table(self):
        result=self.kdb.eval("([]a:til 10;b:reverse til 10;c:10?`4;d:{x#.Q.a}each til 10)")
