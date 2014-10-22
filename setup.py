#!/usr/bin/env python

from __future__ import absolute_import, division, print_function

import os
import sys
import shutil
import textwrap
import pkg_resources
from fnmatch import fnmatchcase

from distutils.core import Command, setup
from distutils.util import convert_path
from distutils.extension import Extension
from distutils.command.build import build
from distutils.command.sdist import sdist
from distutils.command.build_ext import build_ext as _build_ext
from Cython.Distutils import build_ext as _build_ext

#------------------------------------------------------------------------
# Top Level Packages
#------------------------------------------------------------------------

def find_packages(where='.', exclude=()):
    out = []
    stack = [(convert_path(where), '')]
    while stack:
        where, prefix = stack.pop(0)
        for name in os.listdir(where):
            fn = os.path.join(where,name)
            if ('.' not in name and os.path.isdir(fn) and
                os.path.isfile(os.path.join(fn, '__init__.py'))
            ):
                out.append(prefix+name)
                stack.append((fn, prefix+name+'.'))

    if sys.version_info[0] == 3:
        exclude = exclude + ('*py2only*', )

    for pat in list(exclude) + ['ez_setup', 'distribute_setup']:
        out = [item for item in out if not fnmatchcase(item, pat)]

    return out

packages = find_packages()

#------------------------------------------------------------------------
# Minimum Versions
#------------------------------------------------------------------------

#------------------------------------------------------------------------
# Utilities
#------------------------------------------------------------------------

# Some functions for showing errors and warnings.
def _print_admonition(kind, head, body):
    tw = textwrap.TextWrapper(
        initial_indent='   ', subsequent_indent='   ')

    print(".. %s:: %s" % (kind.upper(), head))
    for line in tw.wrap(body):
        print(line)

def exit_with_error(head, body=''):
    _print_admonition('error', head, body)
    sys.exit(1)

def check_import(pkgname, pkgver):
    try:
        mod = __import__(pkgname)
    except ImportError:
            exit_with_error(
                "You need %(pkgname)s %(pkgver)s or greater to run kdbpy!"
                % {'pkgname': pkgname, 'pkgver': pkgver} )
    else:
        if mod.__version__ < pkgver:
            exit_with_error(
                "You need %(pkgname)s %(pkgver)s or greater to run kdbpy!"
                % {'pkgname': pkgname, 'pkgver': pkgver} )

    print("* Found %(pkgname)s %(pkgver)s package installed."
            % {'pkgname': pkgname, 'pkgver': mod.__version__} )
    globals()[pkgname] = mod


#------------------------------------------------------------------------
# Commands
#------------------------------------------------------------------------

class CleanCommand(Command):
    """Custom distutils command to clean the .so and .pyc files."""

    user_options = []

    def initialize_options(self):
        self._clean_me = []
        self._clean_trees = []
        self._clean_exclude = ['c.o']

        for toplevel in packages:
            for root, dirs, files in list(os.walk(toplevel)):
                for f in files:
                    if f in self._clean_exclude:
                        continue
                    if os.path.splitext(f)[-1] in ('.pyc', '.so', '.o', '.pyd'):
                        self._clean_me.append(os.path.join(root, f))

        for d in ('build',):
            if os.path.exists(d):
                self._clean_trees.append(d)

    def finalize_options(self):
        pass

    def run(self):
        for clean_me in self._clean_me:
            try:
                print('flushing', clean_me)
                os.unlink(clean_me)
            except Exception:
                pass
        for clean_tree in self._clean_trees:
            try:
                print('flushing', clean_tree)
                shutil.rmtree(clean_tree)
            except Exception:
                pass

#------------------------------------------------------------------------
# Extensions
#------------------------------------------------------------------------

class build_ext(_build_ext):
    def build_extensions(self):
        numpy_incl = pkg_resources.resource_filename('numpy', 'core/include')

        for ext in self.extensions:
            if hasattr(ext, 'include_dirs') and not numpy_incl in ext.include_dirs:
                ext.include_dirs.append(numpy_incl)
        _build_ext.build_extensions(self)


class CheckingBuildExt(build_ext):
    """Subclass build_ext to get clearer report if Cython is necessary."""

    def check_cython_extensions(self, extensions):
        for ext in extensions:
            for src in ext.sources:
                if not os.path.exists(src):
                    raise Exception("""Cython-generated file '%s' not found.
                Cython is required to compile from a development branch.
                """ % src)

    def build_extensions(self):
        self.check_cython_extensions(self.extensions)
        build_ext.build_extensions(self)


class CythonCommand(build_ext):
    """Custom distutils command subclassed from Cython.Distutils.build_ext
    to compile pyx->c, and stop there. All this does is override the
    C-compile method build_extension() with a no-op."""
    def build_extension(self, ext):
        pass


extensions = []
#import os
#os.environ['CC'] = 'gcc-4.9'
#extensions = [
#    Extension('kdbpy.lib',
#              sources=['kdbpy/lib.pyx','kdbpy/src/k.pxd'],
#              depends=['kdbpy/src/k.h'],
#              extra_objects=['kdbpy/arch/m64/c.o'],
#              libraries=['pthread'],
#              include_dirs=['kdbpy/src'],
#              #extra_compile_args=['-m32']
#              )
#    ]


#------------------------------------------------------------------------
# data files
#------------------------------------------------------------------------
data_files = [('kdbpy/bin/', ['kdbpy/bin/l32/q','kdbpy/bin/m32/q','kdbpy/bin/w32/q.exe'])]

#------------------------------------------------------------------------
# Setup
#------------------------------------------------------------------------

longdesc = open('README.md').read()

setup(
    name='kdbpy',
    version='0.1.1',
    author='Continuum Analytics',
    author_email='kdbpy-dev@continuum.io',
    description='kdbpy',
    long_description=longdesc,
    data_files=data_files,
    license='BSD',
    platforms = ['any'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Education',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities',
    ],
    packages=packages,
    cmdclass = {
        'clean'     : CleanCommand,
        'build_ext' : CheckingBuildExt,
        'cython' : CythonCommand,
        },
    ext_modules=extensions,
)
