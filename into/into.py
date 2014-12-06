from multipledispatch import Dispatcher
from .convert import convert
from .append import append

into = Dispatcher('into')

@into.register(type, object)
def into_type(a, b, **kwargs):
    return convert(a, b)


@into.register(object, object)
def into_object(a, b, **kwargs):
    return append(a, b)
