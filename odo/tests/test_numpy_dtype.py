from __future__ import absolute_import, division, print_function

from odo.numpy_dtype import dshape_to_pandas, unit_to_dtype
import datashape
import numpy as np

def test_parameterized_option_instances():
    dshape1 = datashape.dshape('var * {teststr1: option[string[4]]}')
    dtypes, parse_dates = dshape_to_pandas(dshape1)
    assert isinstance(dtypes['teststr1'], np.dtype)
    
    dshape2 = datashape.dshape('var * {teststr2: option[string["ascii"]]}')
    dtypes, parse_dates = dshape_to_pandas(dshape2)
    assert isinstance(dtypes['teststr2'], np.dtype)
    
    dshape3 = datashape.dshape('option[datetime[tz="EST"]]')
    nptype3 = unit_to_dtype(dshape3)
    assert isinstance(nptype3, np.dtype)
    
    dshape4 = datashape.dshape('option[timedelta[unit="D"]]')
    nptype4 = unit_to_dtype(dshape4)
    assert isinstance(nptype4, np.dtype)
