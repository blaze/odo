from __future__ import print_function, division, absolute_import

from into import discover, CSV, resource, convert
import pandas as pd
from ..utils import cls_name
from toolz import memoize
from datashape import var


class S3(object):
    """An object that holds a resource that lives in an S3 bucket

    Examples
    --------
    >>> bucket = S3(CSV('s3://nyqpug/tips.csv'))
    >>> dshape = discover(bucket)
    >>> dshape.measure['total_bill']
    ctype("float64")

    Notes
    -----
    * pandas read_csv can read CSV files directly from S3 buckets
    * We should check to make sure that we are discovering from CSVs as fast as
      as possible, since using S3 requires hitting the network.
    * For more complicated formats we'll have to write some middleware that
      turns S3 blobs into a format suitable to be converted into other formats
      such as JSON, HDF5, etc. Some of this will be handled by the into graph.
    """
    def __init__(self, data):
        self.data = self.container(data)


@memoize
def s3(cls):
    """Parametrized S3 bucket Class

    Notes
    -----
    * Shamelessly copied from ``into.chunks``
    """
    return type('S3_' + cls_name(cls).replace('.', '_'), (S3,),
                {'container': cls})


@resource.register('s3://.*\.csv', priority=18)
def resource_s3_csv(uri):
    return s3(CSV)(uri)


@convert.register(CSV, s3(CSV))
def convert_s3_to_csv(bucket, **kwargs):
    return CSV(bucket.data.path, **kwargs)


@discover.register(s3(CSV))
def discover_s3_csv(c, **kwargs):
    return var * discover(c.data).subshape[0]


@convert.register(pd.DataFrame, s3(CSV))
def convert_csv_to_s3(csv):
    return convert(pd.DataFrame, csv.data)
