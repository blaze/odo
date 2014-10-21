import unittest
import pytest
from kdbpy import web

class Web(unittest.TestCase):

    def test_start_stop(self):
        w = web.Web().start()
        assert w.is_initialized
        w.stop()
        assert not w.is_initialized
