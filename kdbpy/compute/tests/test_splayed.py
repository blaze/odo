import pytest

from blaze import Data, compute
from kdbpy.compute.qtable import is_splayed


@pytest.fixture(scope='module')
def nbbo(rstring, kdbpar):
    return Data(rstring + '/start/db::nbbo_t')


def test_splayed_nrows(nbbo):
    assert compute(nbbo.nrows) == compute(nbbo.sym.nrows)


def test_splayed_time_type(nbbo):
    assert compute(nbbo.nrows) == compute(nbbo.time.nrows)


def test_is_splayed(nbbo):
    assert is_splayed(nbbo)
