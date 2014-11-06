from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
from contextlib import contextmanager

import pytest
import pandas as pd
import kdbpy
import toolz
from kdbpy import kdb
from kdbpy.kdb import which
import pandas.util.testing as tm
import qpython
import qpython.qwriter


try:
    from cStringIO import StringIO
except ImportError:  # pragma: no cover
    from io import StringIO


def test_basic():
    kq = kdb.KQ()
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
    with kdb.KQ() as kq:
        assert kq.is_started


def test_kq_repr():
    expected = ''
    with kdb.KQ() as kq:
        assert repr(kq) == expected


def test_eval_context():
    with kdb.KQ() as kq:
        assert kq.eval('2 + 2') == 4


def test_credentials():
    cred = kdb.get_credentials(host='foo', port=1000)
    assert cred.host == 'foo'
    assert cred.port == 1000


def test_q_process():
    cred = kdb.get_credentials()
    q = kdb.Q(cred).start()

    assert q is not None
    assert q.pid
    assert q.process is not None

    # other instance
    q2 = kdb.Q(cred).start()
    assert q is q2

    # invalid instance
    c = kdb.get_credentials(host='foo', port=1000)
    with pytest.raises(ValueError):
        kdb.Q(c)

    # restart
    prev = q.pid
    q = kdb.Q(cred).start(start=True)
    assert q.pid == prev

    q2 = kdb.Q().start(start='restart')
    assert q2.pid != prev

    with pytest.raises(ValueError):
        kdb.Q(cred).start(start=False)

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
    return kdb.get_credentials()


@pytest.yield_fixture(scope='module')
def qproc(creds):
    q = kdb.Q(creds).start()
    yield q
    q.stop()


def test_construction(creds):
    k = kdb.KDB(credentials=creds).start()
    assert k.is_started

    # repr
    result = str(k)
    assert '[KDB: Credentials(' in result
    assert '-> connected' in result

    k.stop()
    assert not k.is_started

    result = str(k)
    assert 'KDB: [client/server not started]'

    # require initilization
    cred = kdb.get_credentials(port=0)
    k = kdb.KDB(credentials=cred)
    with pytest.raises(ValueError):
        k.start()


@pytest.yield_fixture(scope='module')
def k(creds, qproc):
    creds = kdb.get_credentials()
    k = kdb.KDB(credentials=creds).start()
    yield k
    k.stop()


def test_eval(k):
    # test function API
    assert k.eval('42') == 42
    f = lambda: k.eval('42') + 1
    assert k.eval(f) == 43
    assert k.eval(lambda x: x+5, 42) == 47


def test_scalar_datetime_like_conversions(k):

    # datetimes
    # only parses to ms resolutions
    result = k.eval('2001.01.01T09:30:00.123')
    assert result == pd.Timestamp('2001-01-01 09:30:00.123')

    result = k.eval('2006.07.04T09:04:59:000')
    assert result == pd.Timestamp('2006-07-04 09:04:59')

    result = k.eval('2001.01.01')
    assert result == pd.Timestamp('2001-01-01')

    # timedeltas
    result = k.eval('00:01')
    assert result == pd.Timedelta('1 min')
    result = k.eval('00:00:01')
    assert result == pd.Timedelta('1 sec')


def test_repr(k):
    expected = ("[KDB: Credentials(host='localhost', port=5001, "
                "username='pcloud', password=None) -> connected]")
    assert repr(k) == expected


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


def test_set_data_frame(kdb, df):
    name = 'gensym'
    kdb.set(name, df)
    result = kdb.eval(name)
    tm.assert_frame_equal(result, df)


@pytest.mark.parametrize('obj', (1, 1.0, 'a'))
def test_set_objects(kdb, obj):
    name = 'gensym'
    kdb.set(name, obj)
    assert kdb.eval(name) == obj


@pytest.mark.xfail(raises=qpython.qwriter.QWriterException,
                   reason='qpython does not implement deserialization of '
                   'complex numbers')
def test_set_complex(kdb):
    name = 'gensym'
    kdb.set(name, 1.0j)
    result = kdb.eval(name)
    assert result == 1.0j
