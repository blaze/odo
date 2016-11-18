#!/usr/bin/env python
from itertools import chain
import os
from fnmatch import fnmatch
from setuptools import setup, find_packages

import versioneer


def find_data_files(where, exts):
    exts = tuple(exts)
    for root, dirs, files in os.walk(where):
        for f in files:
            if any(fnmatch(f, pat) for pat in exts):
                yield os.path.join(root, f)


exts = ('*.h5', '*.csv', '*.xls', '*.xlsx', '*.db', '*.json', '*.gz', '*.hdf5',
        '*.sas7bdat')
package_data = [x.replace('odo' + os.sep, '') for x in
                find_data_files('odo', exts)]


def read(filename):
    with open(filename, 'r') as f:
        return f.read()


def read_reqs(filename):
    return read(filename).strip().splitlines()


def install_requires():
    return read_reqs('etc/requirements.txt')


def extras_require():
    extras = {req: read_reqs('etc/requirements_%s.txt' % req)
              for req in {'aws',
                          'bcolz',
                          'bokeh',
                          'ci',
                          'h5py',
                          'mongo',
                          'mysql',
                          'postgres',
                          'pytables',
                          'sas',
                          'ssh',
                          'sql',
                          'test'}}

    extras['mysql'] += extras['sql']
    extras['postgres'] += extras['sql']

    # don't include the 'ci' or 'test' targets in 'all'
    extras['all'] = list(chain.from_iterable(v for k, v in extras.items()
                                             if k not in {'ci', 'test'}))
    return extras


setup(name='odo',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Data migration utilities',
      url='https://github.com/blaze/odo',
      author='Blaze development team',
      author_email='blaze-dev@continuum.io',
      license='BSD',
      keywords='odo data conversion hdf5 sql blaze',
      packages=find_packages(),
      install_requires=install_requires(),
      extras_require=extras_require(),
      long_description=read('README.rst'),
      package_data={'odo': package_data},
      zip_safe=False,
      scripts=[os.path.join('bin', 'odo')])
