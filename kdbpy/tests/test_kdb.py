import unittest
import pytest
import numpy as np
from kdbpy import kdb

class Lib(unittest.TestCase):

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
        kdb.q_kill_process()
        assert kdb.q_handle is None
