from __future__ import print_function, division, absolute_import

from into import discover, CSV, resource, convert
import pandas as pd
from ..utils import cls_name
from toolz import memoize
from datashape import var


class _S3(object):
    pass


@memoize
def S3(cls):
    """Parametrized S3 bucket Class

    Notes
    -----
    * Shamelessly copied from ``into.chunks``
    """
    return type('S3(%s)' % cls.__name__, (cls, _S3), {'subtype': cls})


@resource.register('s3://.*\.csv', priority=18)
def resource_s3_csv(uri):
    return S3(CSV)(uri)


@convert.register(CSV, S3(CSV))
def convert_s3_to_csv(bucket, **kwargs):
    return CSV(bucket.path, **kwargs)


@discover.register(S3(CSV))
def discover_s3_csv(c, **kwargs):
    return var * discover.dispatch(CSV)(c).subshape[0]
