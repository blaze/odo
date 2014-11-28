import sys
import pytest

import numpy as np
import pandas as pd
import pandas.util.testing as tm
from cStringIO import StringIO

from kdbpy import qmagic


class FakeShell(object):

    def __init__(self):
        self.ns = {}
        self.user_ns = {}
        self.qmagic = qmagic.QMagic(shell=self)

    def run(self, param):
        return self.qmagic.q(param)


@pytest.fixture
def sh():
    return FakeShell()


def test_connect(sh, rstring):
    sh.run('-c %s' % rstring)


def test_restart_connection(sh, rstring):
    sh.run('-r -c %s' % rstring)


def test_debug_arg(sh):
    curstdout = sys.stdout
    try:
        sys.stdout = StringIO()
        sh.run('-d t: 1; t + 1')
        value = sys.stdout.getvalue()
        assert value == 't: 1; t + 1\n'
    finally:
        sys.stdout = curstdout


def test_line_magic(sh):
    assert sh.run('t: 1; t + 1') == 2


def test_maintains_state(sh):
    sh.run('x: 1')
    assert sh.run('x + 1') == 2


def test_multiline(sh):
    q_code = """
t: ([id: til 5];
name: `a`b`c`d`e;
amount: 1.0 2.0 3.0 4.0 5.0);
f: {[x];
    w: x + 1;
    z: w * 2;
    z};
g: value t;
x: f[g.amount];
r: x = (g.amount + 1) * 2;
all r"""
    result = sh.run(q_code)
    assert result


def test_assign_to_python(sh):
    q_code = """-a result
([id: til 5]; name: `a`b`c`d`e; amount: `float$(1 + til 5))"""
    sh.run(q_code)
    expected = pd.DataFrame({'name': list('abcde'),
                             'amount': np.arange(1, 6, dtype='float64')},
                            index=pd.Index(list(range(5)), name='id'))
    result = sh.user_ns['result'].sort_index(axis=1)
    tm.assert_frame_equal(result, expected.sort_index(axis=1))
