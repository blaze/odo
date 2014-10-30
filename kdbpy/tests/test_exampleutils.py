import pytest
import os
import kdbpy

from kdbpy.exampleutils import (module_path, kdbpy_path, example_dir, example,
                                module_dir)


def test_module_path():
    assert os.path.exists(module_path(kdbpy))
    assert os.path.isfile(module_path(kdbpy))

    with pytest.raises(TypeError):
        module_path(pytest.raises)


def test_module_dir():
    assert os.path.exists(module_dir(kdbpy))
    assert os.path.isdir(module_dir(kdbpy))


def test_kdbpy_path():
    assert os.path.exists(kdbpy_path())


def test_example_dir():
    assert os.path.isdir(example_dir())
    assert not os.path.isdir(example_dir('asdf'))


def test_example():
    d = example(os.path.join('data', 'start', 'db'))
    assert os.path.isdir(d)
    assert os.path.getsize(d)
