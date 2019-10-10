#!/usr/bin/env bash

set -xe

# expected to be run from root of repository
cd $GOPATH/src/github.com/m3db/m3

REVISION=$(git rev-parse HEAD)
CLEAN=${CLEAN:-true}
if [[ "$CLEAN" == "true" ]]; then
  make clean
fi
mkdir -p ./bin

# by keeping all the required files in ./bin, it makes the build context
# for docker much smaller
cp ./src/query/config/m3query-dev-remote.yml ./bin
cp ./scripts/comparator/docker-run.sh ./bin

# build images
echo "building docker image"

svc="m3comparator"
echo "creating image for $svc"
make ${svc}-linux-amd64

svc="m3query"
echo "creating image for $svc"
make ${svc}-linux-amd64

docker build -t "comparator:${REVISION}" -f ./scripts/comparator/comparator.Dockerfile ./bin
