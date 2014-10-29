import os
import inspect
import types
import kdbpy


def module_path(module):
    if not isinstance(module, types.ModuleType):
        raise TypeError('object %s is not a module, it is a %r' %
                        (module, type(module).__name__))
    path = inspect.getfile(module)
    if path is None:
        raise ValueError("No path found for module %r" % (module.__name__))
    path, ext = os.path.splitext(path)
    ext = ext.rstrip('c').lstrip(os.extsep)
    fullpath = os.extsep.join((path, ext))
    assert os.path.exists(fullpath)
    return fullpath


def module_dir(module):
    return os.path.dirname(module_path(module))


def kdbpy_path():
    return module_dir(kdbpy)


def example_dir(path='examples'):
    return os.path.join(kdbpy_path(), path)


def example(path, examples_directory='examples'):
    return os.path.join(example_dir(examples_directory), path)


def example_data(path, examples_directory='examples'):
    return os.path.join(example('data', examples_directory=examples_directory),
                        path)
