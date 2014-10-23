import unittest
import pytest
import numpy as np
from kdbpy import kdb

class KQ(unittest.TestCase):

    def test_basic(self):
        kq = kdb.KQ()
        assert not kq.is_started
        kq.start()
        assert kq.is_started
        kq.stop()
        assert not kq.is_started

        kq.start(start='restart')
        assert kq.is_started

        # real restart
        kq.start(start='restart')
        assert kq.is_started

        kq.stop()

        # context
        with kdb.KQ() as kq:
            assert kq.is_started

class QProcess(unittest.TestCase):

    def test_credentials(self):
        cred = kdb.get_credentials(host='foo',port=1000)
        assert cred.host == 'foo'
        assert cred.port == 1000

    def test_q_process(self):
        cred = kdb.get_credentials()
        q = kdb.Q.create(cred).start()

        assert q is not None
        assert q.pid
        assert q.process is not None

        # other instance
        q2 = kdb.Q.create(cred).start()
        assert q is q2

        # invalid instance
        with pytest.raises(ValueError):
            kdb.Q.create(kdb.get_credentials(host='foo',port=1000))

        # restart
        prev = q.pid
        q = kdb.Q.create(cred).start(start=True)
        assert q.pid == prev

        q2 = kdb.Q.create().start(start='restart')
        assert q2.pid != prev

        with pytest.raises(ValueError):
            kdb.Q.create(cred).start(start=False)

        # terminate
        q2.stop()
        assert q2.pid is None

class BasicKDB(unittest.TestCase):

    def setUp(self):
        self.creds = kdb.get_credentials()
        self.q = kdb.Q.create(self.creds).start()

    def tearDown(self):
        self.q.stop()

    def test_construction(self):
        k = kdb.KDB(credentials=self.creds).start()
        assert k.is_started

        # repr
        result = str(k)
        assert 'KDB: [Credentials(' in result
        assert '-> connected' in result

        k.stop()
        assert not k.is_started

        result = str(k)
        assert 'KDB: [client/server not started]'

        # require initilization
        with pytest.raises(ValueError):
            kdb.KDB(credentials=kdb.get_credentials(port=0)).start()

class Eval(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        creds = kdb.get_credentials()
        cls.q = kdb.Q.create(credentials=creds).start()
        cls.kdb = kdb.KDB(credentials=creds).start()

    @classmethod
    def tearDownClass(cls):
        cls.kdb.stop()
        cls.q.stop()
