import pytest

import pandas as pd
import pandas.util.testing as tm
from blaze import compute, into
from kdbpy.compute.qtable import is_splayed


def test_splayed_nrows(par):
    assert compute(par.nbbo_t.nrows) == compute(par.nbbo_t.sym.nrows)


def test_splayed_time_type(par):
    assert compute(par.nbbo_t.nrows) == compute(par.nbbo_t.time.nrows)


def test_is_splayed(par):
    assert is_splayed(par.nbbo_t)


