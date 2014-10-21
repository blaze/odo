import unittest
import pytest
from kdbpy import pcl

class PCL(unittest.TestCase):

    def test_start_kdb(self):
        p = pcl.PCL(start_kdb=True,start_web=False)
        assert p.is_kdb
        assert not p.is_web
        p.stop()
        assert not p.is_kdb
        assert not p.is_web

    def test_start_web(self):
        p = pcl.PCL(start_kdb=False,start_web=True)
        assert not p.is_kdb
        assert p.is_web
        p.stop()
        assert not p.is_kdb
        assert not p.is_web

    def test_start_all(self):
        p = pcl.PCL(start_kdb=True,start_web=True)
        assert p.is_kdb
        assert p.is_web
        p.stop()
        assert not p.is_kdb
        assert not p.is_web
