

import pytest

from into import CSV, cleanup, resource, append, into
from into.utils import raises

def test_notimplemented(tmpdir):

    f1 = str(tmpdir / 'foo.csv')
    csv = CSV(f1, has_header=False)
    data = [('Alice', 100), ('Bob', 200)]
    append(csv, data)

    r = resource(f1)
    assert raises(NotImplementedError, lambda : cleanup(r))

def test_csv(tmpdir):
    # smoke test, cleanup is not implemented

    f1 = str(tmpdir / 'foo.csv')
    f2 = str(tmpdir / 'bar.csv')

    csv = CSV(f1, has_header=False)
    data = [('Alice', 100), ('Bob', 200)]
    append(csv, data)

    into(f2,f1)
