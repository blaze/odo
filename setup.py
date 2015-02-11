#!/usr/bin/env python

import os
from fnmatch import fnmatch
from setuptools import setup, find_packages

def find_data_files(where, exts):
    exts = tuple(exts)
    for root, dirs, files in os.walk(where):
        for f in files:
            if any(fnmatch(f, pat) for pat in exts):
                yield os.path.join(root, f)


exts = ('*.h5', '*.csv', '*.xls', '*.xlsx', '*.db', '*.json', '*.gz', '*.hdf5',
        '*.sas7bdat')
package_data = [x.replace('into' + os.sep, '') for x in
                    find_data_files('into', exts)]


setup(name='into',
      version='0.2.1',
      description='Data migration utilities',
      url='http://github.com/ContinuumIO/into/',
      author='Matthew Rocklin',
      author_email='mrocklin@continuum.io',
      license='BSD',
      keywords='into data conversion hdf5 sql blaze',
      packages=find_packages(),
      install_requires=list(open('requirements.txt').read().strip().split('\n')),
      long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
      package_data={'into': package_data},
      zip_safe=False,
      scripts=['bin/into'])
