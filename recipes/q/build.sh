#!/usr/bin/env sh


SCRIPTS=$PREFIX/bin
rm -rf $SCRIPTS/{q,Q}
mkdir -p $SCRIPTS/Q
cp -rf $SRC_DIR/* $SCRIPTS/Q
rm -rf $SCRIPTS/q
echo '#!/usr/bin/env sh' > $SCRIPTS/q
echo 'export QHOME=$ANACONDA_ENVS/$CONDA_DEFAULT_ENV/bin/Q' >> $SCRIPTS/q
echo '$ANACONDA_ENVS/$CONDA_DEFAULT_ENV/bin/Q/m32/q' >> $SCRIPTS/q
chmod +x $SCRIPTS/q
