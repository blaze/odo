#!/bin/sh

echo "# Usage:"
echo "# \$ eval \$(provisioning.sh)"
echo

TMP_DIR="$HOME/tmp"
MONGO_DOCKER_CID=`docker run -d -p 27017:27017 mongo`
POSTGRES_DOCKER_CID=`docker run -d -v $TMP_DIR:$TMP_DIR -p 5432:5432 -e POSTGRES_DB=test -e POSTGRES_PASSWORD= postgres:9.3`

echo "export POSTGRES_IP="`docker-machine ip default`
echo "export MONGO_IP="`docker-machine ip default`
echo "export POSTGRES_TMP_DIR="$TMP_DIR
echo "export MONGO_DOCKER_CID="$MONGO_DOCKER_CID
echo "export POSTGRES_DOCKER_CID="$POSTGRES_DOCKER_CID
