set -x

# Install dependencies
# Use conda **ONLY** for numpy and pandas (if not pulling from master), this
# speeds up the builds a lot. Use the normal pip install for the rest.
conda create -n odo numpy=1.11.2 python=${TRAVIS_PYTHON_VERSION}
source activate odo

# update setuptools and pip
conda update setuptools pip
if [ -n "$PANDAS_VERSION" ];then
    pip install cython==0.24.1
    pip install $PANDAS_VERSION
else
    conda install pandas=0.19.0
fi

conda install pytables=3.3.0
conda install h5py=2.6.0

# install the frozen ci dependencies
pip install -e .
pip install -r etc/requirements_ci.txt

# datashape
pip install git+git://github.com/blaze/datashape.git
