import unittest
import pytest
from kdbpy import pcl

class PCL(unittest.TestCase):

    def test_start_kdb(self):
        p = pcl.PCL(start_kdb=True)
        assert p.is_kdb
        p.stop()
        assert not p.is_kdb

    def test_examples(self):

        # examples
        import pdb; pdb.set_trace()

        with pcl.PCL() as p:

            for k, e in p.examples:

                result = p.eval(e.q)
                assert isinstance(result, type(e.result))
