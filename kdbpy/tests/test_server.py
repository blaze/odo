import unittest
import pytest
import numpy as np
from kdbpy import server

class Lib(unittest.TestCase):

    def test_credentials(self):
        cred = server.get_credentials(host='foo',port=1000)
        assert cred.host == 'foo'
        assert cred.port == 1000

    def test_q_process(self):
        cred = server.get_credentials()
        q = server.q_start_process(cred)

        assert q is not None
        assert q.pid
        assert server.q_handle is not None

        # restart
        prev = q
        q = server.q_start_process(cred,restart=True)
        assert q.pid != prev.pid

        # invalid restart
        with pytest.raises(ValueError):
            server.q_start_process(cred)

        # terminate
        server.q_kill_process()
        assert server.q_handle is None
