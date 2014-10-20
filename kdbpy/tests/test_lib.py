import unittest
import pytest

import numpy as np
from kdbpy import server, lib

class Base(unittest.TestCase):

    def setUp(self):
        self.creds = server.get_credentials()
        server.q_start_process(self.creds)

    def tearDown(self):
        server.q_kill_process()

class Construction(Base):

    def test_construction(self):
        kdb = lib.KDB(self.creds)
        assert kdb.is_initialized
        kdb.close()
        assert not kdb.is_initialized

        # require initilization
        with pytest.raises(ValueError):
            lib.KDB(server.get_credentials(port=0))

class Eval(Base):

    def setUp(self):
        super(Eval, self).setUp()
        self.kdb = lib.KDB(self.creds)

    def tearDown(self):
        self.kdb.close()
        super(Eval, self).tearDown()

    def test_evaluate_scalar(self):
        import pdb; pdb.set_trace()
        self.kdb.eval("b:42")
        result = self.kdb.eval("b")
        assert result == 42

    def test_evaluate_table(self):
        result=self.kdb.eval("([]a:til 10;b:reverse til 10;c:10?`4;d:{x#.Q.a}each til 10)")
