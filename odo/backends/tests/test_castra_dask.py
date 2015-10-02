from castra import Castra
import dask.dataframe as dd
import pandas as pd
from pandas.util.testing import assert_frame_equal

from odo import odo


def test_dask_roundtrip():
    pdf = pd.DataFrame({'a': [1, 2, 3], 'b': [0.1, 0.2, 0.3]})
    c = odo(pdf, Castra)
    ddf = odo(c, dd.DataFrame)
    assert_frame_equal(ddf.compute(), pdf)
    assert_frame_equal(odo(ddf, Castra)[:], pdf)
