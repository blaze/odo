from into.transitive import *

d = Dispatcher('d')

def test_dispatcher():
    @d.register(int, float)
    def _(_, b):
        return int(b + 1)

    @d.register(str, int)
    def _(_, b):
        return str(b*2)

    assert d(str, 3.0) == str(2*int(3.0 + 1))

    @d.register(str, float)
    def _(_, b):
        return 'direct'

    assert d(str, 3.0) == 'direct'
