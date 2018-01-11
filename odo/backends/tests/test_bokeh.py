from __future__ import absolute_import, division, print_function

import pytest
bokeh = pytest.importorskip('bokeh')

from odo.backends.bokeh import convert, pd, ColumnDataSource
import pandas.util.testing as tm


df = pd.DataFrame([[100, 'Alice'],
                   [200, 'Bob'],
                   [300, 'Charlie']],
                  columns=['balance', 'name'])


def test_convert_dataframe_to_cds():
    cds = convert(ColumnDataSource, df)
    assert list(cds.data['name']) == ['Alice', 'Bob', 'Charlie']
    assert list(cds.data['balance']) == [100, 200, 300]
    df2 = convert(pd.DataFrame, cds)
    assert isinstance(df2, pd.DataFrame)

    tm.assert_frame_equal(df, df2)


