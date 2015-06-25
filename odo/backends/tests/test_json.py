from __future__ import absolute_import, division, print_function

import datetime
import os
import gzip
import os
import json

from contextlib import contextmanager

import numpy as np
from odo.backends.json import json_dumps
from odo.utils import tmpfile, ignoring
from odo import odo, discover, JSONLines, resource, JSON, convert, append, drop
from odo.temp import Temp, _Temp

from datashape import dshape


@contextmanager
def json_file(data):
    with tmpfile('.json') as fn:
        with open(fn, 'w') as f:
            json.dump(data, f, default=json_dumps)

        yield fn


@contextmanager
def jsonlines_file(data):
    with tmpfile('.json') as fn:
        with open(fn, 'w') as f:
            for item in data:
                json.dump(item, f, default=json_dumps)
                f.write('\n')

        yield fn


dat = [{'name': 'Alice', 'amount': 100},
       {'name': 'Bob', 'amount': 200}]


def test_discover_json():
    with json_file(dat) as fn:
        j = JSON(fn)
        assert discover(j) == discover(dat)


def test_discover_jsonlines():
    with jsonlines_file(dat) as fn:
        j = JSONLines(fn)
        assert discover(j) == discover(dat)


def test_discover_json_only_includes_datetimes_not_dates():
    data = [{'name': 'Alice', 'dt': datetime.date(2002, 2, 2)},
            {'name': 'Bob',   'dt': datetime.date(2000, 1, 1)}]
    with json_file(data) as fn:
        j = JSON(fn)
        assert discover(j) == dshape('2 * {dt: datetime, name: string }')


def test_resource():
    with tmpfile('json') as fn:
        assert isinstance(resource('jsonlines://' + fn), JSONLines)
        assert isinstance(resource('json://' + fn), JSON)

        assert isinstance(resource(fn, expected_dshape=dshape('var * {a: int}')),
                          JSONLines)


def test_resource_guessing():
    with json_file(dat) as fn:
        assert isinstance(resource(fn), JSON)

    with jsonlines_file(dat) as fn:
        assert isinstance(resource(fn), JSONLines)


def test_append_jsonlines():
    with tmpfile('json') as fn:
        j = JSONLines(fn)
        append(j, dat)
        with open(j.path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        assert 'Alice' in lines[0]
        assert 'Bob' in lines[1]


def test_append_json():
    with tmpfile('json') as fn:
        j = JSON(fn)
        append(j, dat)
        with open(j.path) as f:
            lines = f.readlines()
        assert len(lines) == 1
        assert 'Alice' in lines[0]
        assert 'Bob' in lines[0]


def test_convert_json_list():
    with json_file(dat) as fn:
        j = JSON(fn)
        assert convert(list, j) == dat


def test_convert_jsonlines():
    with jsonlines_file(dat) as fn:
        j = JSONLines(fn)
        assert convert(list, j) == dat


def test_tuples_to_json():
    ds = dshape('var * {a: int, b: int}')
    with tmpfile('json') as fn:
        j = JSON(fn)

        append(j, [(1, 2), (10, 20)], dshape=ds)
        with open(fn) as f:
            assert '"a": 1' in f.read()

    with tmpfile('json') as fn:
        j = JSONLines(fn)

        append(j, [(1, 2), (10, 20)], dshape=ds)
        with open(fn) as f:
            assert '"a": 1' in f.read()


def test_datetimes():
    from odo import into
    import numpy as np
    data = [{'a': 1, 'dt': datetime.datetime(2001, 1, 1)},
            {'a': 2, 'dt': datetime.datetime(2002, 2, 2)}]
    with tmpfile('json') as fn:
        j = JSONLines(fn)
        append(j, data)

        assert str(into(np.ndarray, j)) == str(into(np.ndarray, data))


def test_json_encoder():
    result = json.dumps([1, datetime.datetime(2000, 1, 1, 12, 30, 0)],
                        default=json_dumps)
    assert result == '[1, "2000-01-01T12:30:00Z"]'
    assert json.loads(result) == [1, "2000-01-01T12:30:00Z"]


def test_empty_line():
    text = '{"a": 1}\n{"a": 2}\n\n'  # extra endline
    with tmpfile('.json') as fn:
        with open(fn, 'w') as f:
            f.write(text)
        j = JSONLines(fn)
        assert len(convert(list, j)) == 2


def test_multiple_jsonlines():
    a, b = '_test_a1.json', '_test_a2.json'
    try:
        with ignoring(OSError):
            os.remove(a)
        with ignoring(OSError):
            os.remove(b)
        with open(a, 'w') as f:
            json.dump(dat, f)
        with open(b'_test_a2.json', 'w') as f:
            json.dump(dat, f)
        r = resource('_test_a*.json')
        result = convert(list, r)
        assert len(result) == len(dat) * 2
    finally:
        with ignoring(OSError):
            os.remove(a)
        with ignoring(OSError):
            os.remove(b)


def test_read_gzip_lines():
    with tmpfile('json.gz') as fn:
        f = gzip.open(fn, 'wb')
        for item in dat:
            s = json.dumps(item).encode('utf-8')
            f.write(s)
            f.write(b'\n')
        f.close()
        js = JSONLines(fn)
        assert convert(list, js) == dat


def test_read_gzip():
    with tmpfile('json.gz') as fn:
        f = gzip.open(fn, 'wb')
        s = json.dumps(dat).encode('utf-8')
        f.write(s)
        f.close()
        js = JSON(fn)
        assert convert(list, js) == dat


def test_write_gzip_lines():
    with tmpfile('json.gz') as fn:
        j = JSONLines(fn)
        append(j, dat)

        f = gzip.open(fn)
        line = next(f)
        f.close()
        assert line.decode('utf-8').strip() == str(json.dumps(dat[0]))


def test_write_gzip():
    with tmpfile('json.gz') as fn:
        j = JSON(fn)
        append(j, dat)

        f = gzip.open(fn)
        text = f.read()
        f.close()
        assert text.decode('utf-8').strip() == str(json.dumps(dat))
        assert isinstance(resource(fn), (JSON, JSONLines))


def test_resource_gzip():
    with tmpfile('json.gz') as fn:
        assert isinstance(resource(fn), (JSON, JSONLines))
        assert isinstance(resource('json://' + fn), (JSON, JSONLines))
        assert isinstance(resource('jsonlines://' + fn), (JSON, JSONLines))

    with tmpfile('jsonlines.gz'):
        assert isinstance(resource('jsonlines://' + fn), (JSON, JSONLines))


def test_convert_to_temp_json():
    js = convert(Temp(JSON), [1, 2, 3])
    assert isinstance(js, JSON)
    assert isinstance(js, _Temp)

    assert convert(list, js) == [1, 2, 3]


def test_drop():
    with tmpfile('json') as fn:
        js = JSON(fn)
        append(js, [1, 2, 3])

        assert os.path.exists(fn)
        drop(js)
        assert not os.path.exists(fn)


def test_missing_to_csv():
    data = [dict(a=1, b=2), dict(a=2, c=4)]
    with tmpfile('.json') as fn:
        js = JSON(fn)
        js = odo(data, js)

        with tmpfile('.csv') as csvf:
            csv = odo(js, csvf)
            with open(csv.path, 'rt') as f:
                result = f.read()

    expected = 'a,b,c\n1,2.0,\n2,,4.0\n'
    assert result == expected
