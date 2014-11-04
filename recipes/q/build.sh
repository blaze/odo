#!/usr/bin/env sh


SCRIPTS=$PREFIX/bin
cp -rf $SRC_DIR $SCRIPTS/q
echo 'export QHOME=$ANACONDA_ENVS/$CONDA_DEFAULT_ENV/bin/q' > $SCRIPTS/q
echo '$ANACONDA_ENVS/$CONDA_DEFAULT_ENV/bin/q/m32/q' >> $SCRIPTS/q
