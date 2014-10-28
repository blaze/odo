import pytest
from blaze import Data
from kdbpy.compute.qtable import ispartitioned


@pytest.fixture
def trade(rstring, kdbpar):
    return Data(rstring + '/start/db/::trade', engine=kdbpar)


def test_ispartitioned(trade):
    assert ispartitioned(trade)


def test_head(trade):
    assert repr(trade)


@pytest.mark.xfail
def test_field(trade):
    assert repr(trade.price)
