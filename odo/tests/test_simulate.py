from __future__ import absolute_import, division, print_function

import unittest

import pandas as pd
import networkx as nx

from odo import odo, CSV, JSON
from odo.utils import filetext

class TestAppendSimulation(unittest.TestCase):
    """Run odo with simulate=True for some append cases."""

    def test_csv_to_json_append(self):
        with filetext('alice,1\nbob,2', extension='.csv') as source:
            path = odo(CSV(source), JSON('dummy_to.json'), simulate=True)
            self.assertTrue(isinstance(path.source, CSV))
            self.assertTrue(isinstance(path.target, JSON))
            self.assertEquals(path.append_sig[0], JSON)
            self.assertEquals(path.append_sig[1], CSV)
            self.assertEquals(path.append_func.__name__, 'object_to_json')
            self.assertTrue('def object_to_json' in path.append_code)
            self.assertEquals(path.convert_sig[0], list)
            self.assertEquals(path.convert_sig[1], CSV)
            self.assertEquals(len(path.convert_path), 3)
            self.assertEquals(path.convert_path[0].source, CSV)
            self.assertEquals(path.convert_path[2].target, list)

    def test_csv_to_list_append(self):
        with filetext('alice,1\nbob,2', extension='.csv') as source:
            path = odo(CSV(source), [1, 2, 3], simulate=True)
            self.assertTrue(isinstance(path.source, CSV))
            self.assertTrue(isinstance(path.target, list))
            self.assertEquals(path.append_sig[0], list)
            self.assertEquals(path.append_sig[1], CSV)
            self.assertEquals(path.append_func.__name__, 'object_to_list')
            self.assertTrue('append(a, convert(list, b))' in path.append_code)
            self.assertEquals(path.convert_sig[0], list)
            self.assertEquals(path.convert_sig[1], CSV)
            self.assertEquals(len(path.convert_path), 3)
            self.assertEquals(path.convert_path[0].source, CSV)
            self.assertEquals(path.convert_path[2].target, list)

    def test_list_to_set_append(self):
        s = set([1, 2, 3])
        path = odo([4, 5, 6], s, simulate=True)
        self.assertTrue(isinstance(path.source, list))
        self.assertTrue(isinstance(path.target, set))
        self.assertEquals(path.append_sig[0], set)
        self.assertEquals(path.append_sig[1], list)
        self.assertEquals(path.append_func.__name__, 'object_to_set')
        self.assertTrue('def object_to_set(a, b, **kwargs)' in path.append_code)
        self.assertEquals(path.convert_sig[0], set)
        self.assertEquals(path.convert_sig[1], list)
        self.assertEquals(len(path.convert_path), 1)
        self.assertEquals(path.convert_path[0].source, list)
        self.assertEquals(path.convert_path[0].target, set)

class TestConvertSimulation(unittest.TestCase):
    """Run odo with simulate=True for some convert cases."""

    def test_csv_to_dataframe_convert(self):
        with filetext('alice,1\nbob,2', extension='.csv') as source:
            path = odo(CSV(source), pd.DataFrame, simulate=True)
            self.assertTrue(isinstance(path.source, CSV))
            self.assertEquals(path.target, pd.DataFrame)
            self.assertTrue(path.append_sig is None)
            self.assertEquals(path.convert_sig[0], pd.DataFrame)
            self.assertEquals(path.convert_sig[1], CSV)
            self.assertEquals(len(path.convert_path), 2)
            self.assertEquals(path.convert_path[0].source, CSV)
            self.assertEquals(path.convert_path[0].target.__name__,
                              'chunks(pandas.DataFrame)')
            self.assertEquals(path.convert_path[0].func.__name__,
                              'CSV_to_chunks_of_dataframes')

    def test_json_object_to_csv_type(self):
        with filetext('[{"alice": 1, "bob": 2}]', extension='.json') as source:
            with self.assertRaises(nx.NetworkXNoPath):
                path = odo(JSON(source), CSV)


if __name__ == '__main__':
    unittest.main()
