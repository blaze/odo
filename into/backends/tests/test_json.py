from into.backends.json import *
from into.utils import tmpfile
from contextlib import contextmanager
from datashape import dshape
import json

@contextmanager
def json_file(data):
    with tmpfile('.json') as fn:
        with open(fn, 'w') as f:
            json.dump(data, f)

        yield fn

@contextmanager
def jsonlines_file(data):
    with tmpfile('.json') as fn:
        with open(fn, 'w') as f:
            for item in data:
                json.dump(item, f)
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


def test_resource():
    assert isinstance(resource('jsonlines://foo.json'), JSONLines)
    assert isinstance(resource('json://foo.json'), JSON)

    assert isinstance(resource('foo.json', dshape=dshape('var * {a: int}')),
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
