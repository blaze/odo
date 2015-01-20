from __future__ import absolute_import, division, print_function

import pytest
sas7bdat = pytest.importorskip('sas7bdat')
pytest.importorskip('into.backends.sas')
import os
import pandas as pd
import numpy as np
import datashape
from collections import Iterator
from sas7bdat import SAS7BDAT

from into.backends.sas import discover, sas_to_DataFrame, sas_to_iterator
from into.utils import tmpfile
from into import append, convert, resource, dshape

cur_path = os.path.abspath(os.path.dirname(__file__))
test_path = os.path.join(cur_path, 'airline.sas7bdat')


@pytest.yield_fixture
def sasfile():
    with SAS7BDAT(test_path) as f:
        yield f


columns = ("DATE", "AIR", "mon1", "mon2", "mon3", "mon4", "mon5", "mon6",
           "mon7", "mon8", "mon9", "mon10", "mon11", "mon12", "t", "Lair")


def test_resource_sas7bdat(sasfile):
    assert isinstance(resource(test_path), SAS7BDAT)


def test_discover_sas(sasfile):
    ds = ", ".join(col + ": float64" for col in columns)
    expected = dshape("var * {" + ds + "}")
    ans = discover(sasfile)
    assert discover(sasfile) == expected


def test_convert_sas_to_dataframe(sasfile):
    df = sas_to_DataFrame(sasfile)
    assert set(df.columns) == set(columns)
    assert all([df[col].dtype == np.dtype('float64') for col in df.columns])


def test_convert_sas_to_list(sasfile):
    out = convert(list, sasfile)
    assert isinstance(out, list)
    assert all(isinstance(ln, list) for ln in out)


def test_convert_sas_to_iterator(sasfile):
    itr = sas_to_iterator(sasfile)
    assert isinstance(itr, Iterator)


def test_append_sas_to_sqlite_round_trip(sasfile):
    expected = convert(set, sasfile)

    with tmpfile('db') as fn:
        r = resource('sqlite:///%s::A', dshape=discover(sasfile))
        append(r, sasfile)

        result = convert(set, r)

    assert expected == result
