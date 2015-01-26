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


@pytest.mark.xfail(raises=NotImplementedError,
                   reason='not implemented for splayed tables')
def test_append_frame_to_splayed(par):
    tablename = par.nbbo_t._name
    df = par.data.eval(tablename)
    expected = pd.concat([df, df], ignore_index=True)
    result = into(pd.DataFrame, into(par.nbbo_t, df))
    tm.assert_frame_equal(result, expected)
