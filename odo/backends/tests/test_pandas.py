from __future__ import absolute_import, division, print_function

import os
from datashape import discover, Option, String
from odo.backends.pandas import discover
import pandas as pd
from datashape import dshape
from odo import discover, odo

data = [('Alice', 100), ('Bob', 200)]


def test_discover_dataframe():
    df = pd.DataFrame(data, columns=['name', 'amount'])
    assert discover(df) == dshape("2 * {name: string[5, 'A'], amount: int64}")


def test_discover_series():
    s = pd.Series([1, 2, 3])

    assert discover(s) == 3 * discover(s[0])


def test_discover_ascii_string_series():
    s = pd.Series(list('abc'))
    assert discover(s) == 3 * String(1, 'A')


def test_discover_unicode_string_series():
    s = pd.Series(list(u'abc'))
    assert discover(s) == 3 * String(1, 'U8')


def test_floats_are_optional():
    df = pd.DataFrame([('Alice', 100), ('Bob', None)],
                      columns=['name', 'amount'])
    ds = discover(df)
    assert isinstance(ds[1].types[1], Option)


def test_trip_small_csv_discover_not_equal_to_dataframe_discover():
    filename = os.path.join(os.path.dirname(__file__), 'tripsmall.csv')
    result = discover(odo(filename, pd.DataFrame))
    expected = dshape("""30 * {
      medallion: string[32],
      hack_license: string[32],
      vendor_id: string[3],
      rate_code: int64,
      store_and_fwd_flag: string[1],
      pickup_datetime: ?datetime,
      dropoff_datetime: ?datetime,
      passenger_count: int64,
      trip_time_in_secs: int64,
      trip_distance: ?float64,
      pickup_longitude: ?float64,
      pickup_latitude: ?float64,
      dropoff_longitude: ?float64,
      dropoff_latitude: ?float64,
      tolls_amount: ?float64,
      tip_amount: ?float64,
      total_amount: ?float64,
      mta_tax: ?float64,
      fare_amount: ?float64,
      payment_type: string[3],
      surcharge: ?float64
    }""")
    assert result == expected
