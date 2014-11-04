#!/usr/bin/env sh


qlib=$PREFIX/lib/q
mkdir -p $qlib
mkdir -p $PREFIX/bin
cp -rf $SRC_DIR/* $qlib


[ `uname` = "Darwin" ] && arch=m || arch=l

touch $PREFIX/bin/q
chmod +x $PREFIX/bin/q

cat <<EOF > $PREFIX/bin/q
#!/usr/bin/env sh
export QHOME=/opt/anaconda1anaconda2anaconda3/lib/q

qpath=\$QHOME/${arch}32/q
if [ ! -x "\$qpath" ]; then
    chmod +x "\$qpath"
fi

\$qpath

EOF
