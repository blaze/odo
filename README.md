# `kdbpy`: The performance of kdb+ without the inconvenience of `q`

[![Binstar Badge](https://binstar.org/cpcloud/kdbpy/badges/build.svg)](https://binstar.org/cpcloud/kdbpy/builds)
[![Binstar Badge](https://binstar.org/cpcloud/kdbpy/badges/version.svg)](https://binstar.org/cpcloud/kdbpy)
[![Binstar Badge](https://binstar.org/cpcloud/kdbpy/badges/installer/conda.svg)](https://conda.binstar.org/cpcloud)

## What it is

* A way to drive the KDB+ database using Python.

## Main Features

* All the benefits of the fast columnar [KDB+ database](http://kx.com/kdb-plus.php)
  without the annoyance of the [`q`
  language](http://en.wikipedia.org/wiki/Q_(programming_language_from_Kx_Systems)).
* Integrates well with other libraries in the data-oriented python
  ecosystem (e.g., `pandas`, `numpy`)
* Easy migrations from KDB+ to other formats and in-memory stores
  through the use of `blaze`.
* Handles startup and teardown of a single `q` process.
* An [IPython](http://www.ipython.org)
  [magic](http://ipython.org/ipython-doc/stable/interactive/tutorial.html#magic-functions)
  for running `q` expressions in a notebook.


## Where to get it

You can install the package from [binstar.org](http://www.binstar.org)
using [`conda`](http://conda.pydata.org):

```sh
conda install -c cpcloud -c blaze kdbpy
```

The source code is availabe at https://github.com/ContinuumIO/kdbpy

## Dependencies

**Note:** *Every dependency listed here can be obtained using*
[`conda`](http://conda.pydata.org):

```sh
conda install <name-of-dependency>
```

### Required Dependencies

* [Python 2.7.8](http://www.python.org): The Python programming language
* [`blaze`](https://github.com/ContinuumIO/blaze): Composable
  abstractions for data
* [`q`](http://github.com/ContinuumIO/conda-q): The `q` interpreter
* [`qpython`](https://github.com/exxceleron/qPython): Talk to the `q`
  interpreter over a TCP socket
* [`ipython`](http://www.ipython.org): Awesome interactive shell for Python


### Optional Dependencies

* [`ipython-notebook`](http://ipython.org/notebook.html): Needed to use
  the `%q` and `%%q` magics.


### Development Dependencies

In addition to the [Required](#required-dependencies) and
[Optional](#optional-dependencies) deps you need the following
libraries:

* [`pytest`](http://www.pytest.org): For testing

## Installation from sources

To install from source you need Cython in addition to the normal
dependencies above.

The easiest way to install from source is to install via `conda` as
above, uninstall the package, then build the `conda` recipe and install
the resulting build via the `--use-local` flag.

```sh
conda install cython pytest kdbpy -c cpcloud -c blaze
conda remove kdbpy
cd kdbpy
conda build conda.recipe --python=2.7
conda install kdbpy --use-local
```
