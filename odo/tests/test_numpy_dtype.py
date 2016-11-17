from __future__ import absolute_import, division, print_function

import pytest

from odo.numpy_dtype import dshape_to_pandas, unit_to_dtype, dshape_to_numpy
from datashape import dshape
import numpy as np


@pytest.mark.parametrize(
    ['ds', 'expected'],
    [
        ('decimal[9,2]', np.float64),
        ('decimal[9]', np.int32),
        ('?decimal[9]', np.float32),
        ('?decimal[1,0]', np.float16),
    ]
)
def test_decimal(ds, expected):
    assert unit_to_dtype(dshape(ds)) == expected


@pytest.mark.parametrize(
    ['ds', 'field'],
    [('var * {teststr1: option[string[4]]}', 'teststr1'),
     ('var * {teststr2: option[string["ascii"]]}', 'teststr2')]
)
def test_parameterized_option_instances(ds, field):
    dtypes, _ = dshape_to_pandas(dshape(ds))
    assert isinstance(dtypes[field], np.dtype)


@pytest.mark.parametrize(
    'ds',
    [
        'option[datetime[tz="EST"]]',
        'option[timedelta[unit="D"]]'
    ]
)
def test_unit_to_dtype(ds):
    assert isinstance(unit_to_dtype(ds), np.dtype)


@pytest.mark.parametrize(
    ['ds', 'expected'],
    [
        ('{a: int32}', ({'a': np.dtype('int32')}, [])),
        ('{a: int32, when: datetime}', ({'a': np.dtype('int32')}, ['when'])),
        ('{a: ?int64}', ({'a': np.dtype('float64')}, []))
    ]
)
def test_dshape_to_pandas(ds, expected):
    assert dshape_to_pandas(ds) == expected


@pytest.mark.parametrize(
    ['ds', 'dt'],
    [
        ('int32', 'int32'),
        ('?int32', 'float32'),
        (
            '{name: string[5, "ascii"], amount: ?int32}',
            [('name', 'S5'), ('amount', '<f4')]
        ),
        ('(int32, float32)', [('f0', '<i4'), ('f1', '<f4')])
    ]
)
def test_dshape_to_numpy(ds, dt):
    assert dshape_to_numpy(ds) == np.dtype(dt)
