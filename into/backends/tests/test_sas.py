from __future__ import absolute_import, division, print_function

import pytest
sas7bdat = pytest.importorskip('sas7bdat')
pytest.importorskip('odo.backends.sas')
import os
import pandas as pd
from collections import Iterator
from sas7bdat import SAS7BDAT

from odo.backends.sas import discover, sas_to_iterator
from odo.utils import tmpfile, into_path
from odo import append, convert, resource, dshape


test_path = into_path('backends', 'tests', 'airline.sas7bdat')
sasfile = SAS7BDAT(test_path)


columns = ("DATE", "AIR", "mon1", "mon2", "mon3", "mon4", "mon5", "mon6",
           "mon7", "mon8", "mon9", "mon10", "mon11", "mon12", "t", "Lair")

ds = dshape('''var * {DATE: date, AIR: float64, mon1: float64, mon2: float64,
                      mon3: float64, mon4: float64, mon5: float64,
                      mon6: float64, mon7: float64, mon8: float64,
                      mon9: float64, mon10: float64, mon11: float64,
                      mon12: float64, t: float64, Lair: float64}''')


def test_resource_sas7bdat():
    assert isinstance(resource(test_path), SAS7BDAT)


def test_discover_sas():
    assert discover(sasfile) == ds


def test_convert_sas_to_dataframe():
    df = convert(pd.DataFrame, sasfile)
    assert isinstance(df, pd.DataFrame)

    # pandas doesn't support date
    expected = str(ds.measure).replace('date', 'datetime')

    assert str(discover(df).measure).replace('?', '') == expected


def test_convert_sas_to_list():
    out = convert(list, sasfile)
    assert isinstance(out, list)
    assert not any(isinstance(item, str) for item in out[0])  # No header
    assert all(isinstance(ln, list) for ln in out)


def test_convert_sas_to_iterator():
    itr = sas_to_iterator(sasfile)
    assert isinstance(itr, Iterator)


def test_append_sas_to_sqlite_round_trip():
    expected = convert(set, sasfile)

    with tmpfile('db') as fn:
        r = resource('sqlite:///%s::SAS' % fn, dshape=discover(sasfile))
        append(r, sasfile)

        result = convert(set, r)

    assert expected == result
