from __future__ import absolute_import, division, print_function

from odo.regex import RegexDispatcher

import re


def test_regex_dispatcher():
    foo = RegexDispatcher('foo')

    @foo.register(r'\d*', priority=9)
    def a(s):
        """ A's docstring """
        return int(s)

    @foo.register(re.compile(r'\D*'))
    def b(s):
        """ B's docstring """
        return s

    @foo.register(r'0\d*', priority=11)
    def c(s):
        """ C's docstring """
        return s

    assert set(foo.funcs.values()) == set([a, b, c])
    assert foo.dispatch('123') == a
    assert foo.dispatch('hello') == b
    assert foo.dispatch('0123') == c

    assert foo('123') == 123
    assert foo('hello') == 'hello'
    assert foo('0123') == '0123'

    assert a.__doc__ in foo.__doc__
