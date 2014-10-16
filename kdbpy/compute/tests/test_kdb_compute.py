import getpass
import pytest

import numpy as np
from blaze import Symbol, compute
from kdbpy import into, resource


@pytest.fixture(scope='module')
def raw():
    return np.array([(1, -100.90, 'Bob'),
                     (2, 300.23, 'Alice'),
                     (3, 432.2, 'Joe')],
                    dtype=[('id', 'int64'),
                           ('amount', 'float64'),
                           ('name', 'object')])


@pytest.fixture(scope='module')
def kdb(raw):
    r = resource('kdb://{0}@localhost::test'.format(getpass.getuser()))
    return into(r, raw)


def test_projection(kdb, raw):
    t = Symbol('t', 'var * {id: int, amount: float64, name: string}')
    expr = t[['a', 'b']]
    result = compute(expr, kdb)
    assert np.array_equal(result, raw)
