from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
from contextlib import contextmanager

import pytest
import pandas as pd
import pandas.util.testing as tm
import numpy as np
import toolz
import kdbpy

from blaze import CSV

import qpython
import qpython.qwriter

from kdbpy import kdb as k
from kdbpy.kdb import which

try:
    from cStringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO


def test_basic():
    kq = k.KQ()
    assert not kq.is_started
    kq.start()
    assert kq.is_started
    kq.stop()
    assert not kq.is_started

    kq.start(start='restart')
    assert kq.is_started

    # real restart
    kq.start(start='restart')
    assert kq.is_started

    kq.stop()

    # context
    with k.KQ() as kq:
        assert kq.is_started


def test_kq_repr():
    expected = ''
    with k.KQ() as kq:
        assert repr(kq) == expected


def test_eval_context():
    with k.KQ() as kq:
        assert kq.eval('2 + 2') == 4


def test_credentials():
    cred = k.get_credentials(host='foo', port=1000)
    assert cred.host == 'foo'
    assert cred.port == 1000


def test_q_process():
    cred = k.get_credentials()
    q = k.Q(cred).start()

    assert q is not None
    assert q.pid
    assert q.process is not None

    # other instance
    q2 = k.Q(cred).start()
    assert q is q2

    # invalid instance
    c = k.get_credentials(host='foo', port=1000)
    with pytest.raises(ValueError):
        k.Q(c)

    # restart
    prev = q.pid
    q = k.Q(cred).start(start=True)
    assert q.pid == prev

    q2 = k.Q().start(start='restart')
    assert q2.pid != prev

    with pytest.raises(ValueError):
        k.Q(cred).start(start=False)

    # terminate
    q2.stop()
    assert q2.pid is None


def test_q_process_detached(qproc):

    # create a new process
    assert qproc is not None
    assert qproc.pid
    assert qproc.process is not None

    qproc.process = None


@pytest.fixture(scope='module')
def creds():
    return k.get_credentials()


@pytest.yield_fixture(scope='module')
def qproc(creds):
    q = k.Q(creds).start()
    yield q
    q.stop()


def test_construction(creds):
    kdb = k.KDB(credentials=creds).start()
    assert kdb.is_started

    # repr
    result = str(kdb)
    assert '[KDB: Credentials(' in result
    assert '-> connected' in result

    kdb.stop()
    assert not kdb.is_started

    result = str(kdb)
    assert 'KDB: [client/server not started]'

    # require initilization
    cred = k.get_credentials(port=0)
    kdb = k.KDB(credentials=cred)
    with pytest.raises(ValueError):
        kdb.start()


def test_eval(kdb):
    # test function API
    assert kdb.eval('42') == 42
    f = lambda: kdb.eval('42') + 1
    assert kdb.eval(f) == 43
    assert kdb.eval(lambda x: x+5, 42) == 47


def test_scalar_datetime_like_conversions(kdb):

    # datetimes
    # only parses to ms resolutions
    result = kdb.eval('2001.01.01T09:30:00.123')
    assert result == pd.Timestamp('2001-01-01 09:30:00.123')

    result = kdb.eval('2006.07.04T09:04:59:000')
    assert result == pd.Timestamp('2006-07-04 09:04:59')

    result = kdb.eval('2001.01.01')
    assert result == pd.Timestamp('2001-01-01')

    # timedeltas
    result = kdb.eval('00:01')
    assert result == pd.Timedelta('1 min')
    result = kdb.eval('00:00:01')
    assert result == pd.Timedelta('1 sec')


def test_repr_smoke(kdb):
    assert repr(kdb)


def test_print_versions():
    file = StringIO()
    kdbpy.print_versions(file=file)


@contextmanager
def remove_from_path(path):
    current_path = os.environ['PATH']
    new_path = list(toolz.unique(current_path.split(os.pathsep)))
    new_path.pop(new_path.index(path))
    os.environ['PATH'] = os.pathsep.join(new_path)
    yield
    os.environ['PATH'] = current_path


def test_cannot_find_q():
    remove_this = os.path.dirname(which('q'))
    with remove_from_path(remove_this):
        with pytest.raises(OSError):
            which('q')


def test_set_data_frame(gensym, kdb, df):
    kdb.set(gensym, df)
    result = kdb.eval(gensym)
    tm.assert_frame_equal(result, df)


@pytest.mark.parametrize('obj', (1, 1.0, 'a'))
def test_set_objects(gensym, kdb, obj):
    kdb.set(gensym, obj)
    assert kdb.eval(gensym) == obj


@pytest.mark.xfail(raises=qpython.qwriter.QWriterException,
                   reason='qpython does not implement deserialization of '
                   'complex numbers')
def test_set_complex(gensym, kdb):
    kdb.set(gensym, 1.0j)
    result = kdb.eval(gensym)
    assert result == 1.0j


def test_date(kdb, gensym):
    csvdata = """name,date
a,2010-10-01
b,2010-10-02
c,2010-10-03
d,2010-10-04
e,2010-10-05"""
    with tm.ensure_clean('tmp.csv') as fname:
        with open(fname, 'wb') as f:
            f.write(csvdata)

        dshape = CSV(fname, header=0).dshape
        kdb.read_csv(fname, table=gensym, dshape=dshape)

        expected = pd.read_csv(fname, header=0, parse_dates=['date'])
    result = kdb.eval(gensym)
    tm.assert_frame_equal(expected, result)


def test_timestamp(kdb, gensym):
    csvdata = """name,date
a,2010-10-01 00:00:05
b,2010-10-02 00:00:04
c,2010-10-03 00:00:03
d,2010-10-04 00:00:02
e,2010-10-05 00:00:01"""
    with tm.ensure_clean('tmp.csv') as fname:
        with open(fname, 'wb') as f:
            f.write(csvdata)

        dshape = CSV(fname, header=0).dshape
        kdb.read_csv(fname, table=gensym, dshape=dshape)

        expected = pd.read_csv(fname, header=0, parse_dates=['date'])
    result = kdb.eval(gensym)
    tm.assert_frame_equal(expected, result)


@pytest.mark.xfail(raises=AssertionError,
                   reason="Can't parse D timestamp as timestamps yet")
def test_write_timestamp_from_q(kdb, gensym):
    csvdata = """name,date
a,2010-10-01D00:00:05
b,2010-10-02D00:00:04
c,2010-10-03D00:00:03
d,2010-10-04D00:00:02
e,2010-10-05D00:00:01"""
    with tm.ensure_clean('tmp.csv') as fname:
        with open(fname, 'wb') as f:
            f.write(csvdata)

        dshape = CSV(fname, header=0).dshape
        kdb.read_csv(fname, table=gensym, dshape=dshape)

        expected = pd.read_csv(fname, header=0, parse_dates=['date'])
    result = kdb.eval(gensym)
    tm.assert_frame_equal(expected, result)
    assert expected.date.dtype == np.dtype('datetime64[ns]')
