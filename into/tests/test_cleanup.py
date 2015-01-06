

import pytest

from into import CSV, cleanup, resource, append, into
from into.utils import raises


def test_not_impemented():

    # these are by definition not implemented
    for obj in ['foo', None, object]:
        cleanup(obj)


def test_csv_notimplemented(tmpdir):

    f1 = str(tmpdir / 'foo.csv')
    csv = CSV(f1, has_header=False)
    data = [('Alice', 100), ('Bob', 200)]
    append(csv, data)

    r = resource(f1)
    cleanup(r)


def test_csv(tmpdir):
    # smoke test, cleanup is not implemented

    f1 = str(tmpdir / 'foo.csv')
    f2 = str(tmpdir / 'bar.csv')

    csv = CSV(f1, has_header=False)
    data = [('Alice', 100), ('Bob', 200)]
    append(csv, data)

    result = into(f2, f1)
    assert isinstance(result, CSV)
