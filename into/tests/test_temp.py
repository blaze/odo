from __future__ import absolute_import, division, print_function

from odo.temp import Temp, _Temp
from odo.drop import drop

class Foo(object):
    pass


flag = [0]


@drop.register(Foo)
def drop_foo(foo, **kwrags):
    flag[0] = flag[0] + 1


def test_Temp():
    foo = Temp(Foo)()

    assert isinstance(foo, Foo)
    assert isinstance(foo, _Temp)

    x = flag[0]
    del foo
    assert flag[0] == x + 1
