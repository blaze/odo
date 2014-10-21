import unittest
import pytest
import numpy as np
from kdbpy import kdb

class QProcess(unittest.TestCase):

    def test_credentials(self):
        cred = kdb.get_credentials(host='foo',port=1000)
        assert cred.host == 'foo'
        assert cred.port == 1000

    def test_q_process(self):
        cred = kdb.get_credentials()
        q = kdb.q_start_process(cred)

        assert q is not None
        assert q.pid
        assert kdb.q_handle is not None

        # restart
        prev = q
        q = kdb.q_start_process(cred,restart=True)
        assert q.pid != prev.pid

        # invalid restart
        with pytest.raises(ValueError):
            kdb.q_start_process(cred)

        # terminate
        kdb.q_stop_process()
        assert kdb.q_handle is None

class BasicKDB(unittest.TestCase):

    def setUp(self):
        self.creds = kdb.get_credentials()
        kdb.q_start_process(self.creds)

    def tearDown(self):
        kdb.q_stop_process()

    def test_construction(self):
        k = kdb.KDB(self, self.creds).start()
        assert k.is_initialized
        k.stop()
        assert not k.is_initialized

        # require initilization
        with pytest.raises(ValueError):
            kdb.KDB(self, kdb.get_credentials(port=0)).start()

class Eval(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.creds = kdb.get_credentials()
        kdb.q_start_process(cls.creds)
        cls.kdb = kdb.KDB(cls, cls.creds).start()

    @classmethod
    def tearDownClass(cls):
        cls.kdb.stop()
        kdb.q_stop_process()

    def test_evaluate_scalar(self):
        result = self.kdb.eval("42")
        assert result == 42

    def test_evaluate_table(self):
        result=self.kdb.eval("([]a:til 10;b:reverse til 10;c:10?`4;d:{x#.Q.a}each til 10)")
