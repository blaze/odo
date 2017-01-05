from itertools import chain
import os
import sys
from fnmatch import fnmatch
from setuptools import setup, find_packages


def find_data_files(where, exts):
    exts = tuple(exts)
    for root, _, files in os.walk(where):
        for f in files:
            if any(fnmatch(f, pat) for pat in exts):
                yield os.path.join(root, f)

def get_package_data():
    exts = ('*.h5', '*.csv', '*.xls', '*.xlsx', '*.db', '*.json', '*.gz', '*.hdf5',
            '*.sas7bdat', '*.vcf')
    return [x.replace('odo' + os.sep, '') for x in
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

def setup_package():
    src_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    old_path = os.getcwd()
    os.chdir(src_path)
    sys.path.insert(0, src_path)


    needs_pytest = {'pytest', 'test', 'ptr'}.intersection(sys.argv)
    pytest_runner = ['pytest-runner'] if needs_pytest else []

    setup_requires = [] + pytest_runner
    tests_require = ['pytest'] + extras_require()['ssh']

    metadata = dict(
        name='odo',
        version='0.5.2.dev0',
        description='Data migration utilities',
        url='https://github.com/blaze/odo',
        author='Blaze development team',
        author_email='blaze-dev@continuum.io',
        license='BSD',
        keywords='odo data conversion hdf5 sql blaze',
        packages=find_packages(),
        install_requires=install_requires(),
        setup_requires=setup_requires,
        tests_require=tests_require,
        extras_require=extras_require(),
        long_description=read('README.rst'),
        package_data={'odo': get_package_data()},
        include_package_data=True,
        zip_safe=False,
        scripts=[os.path.join('bin', 'odo')],
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "License :: OSI Approved :: BSD License",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3.4",
            "Programming Language :: Python :: 3.5",
            "Operating System :: OS Independent",
        ],
    )

    try:
        setup(**metadata)
    finally:
        del sys.path[0]
        os.chdir(old_path)

if __name__ == '__main__':
    setup_package()
