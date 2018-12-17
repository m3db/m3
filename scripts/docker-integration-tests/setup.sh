#!/usr/bin/env bash

set -xe

# expected to be run from root of repository
cd $GOPATH/src/github.com/m3db/m3

SERVICES=(m3dbnode m3coordinator)
REVISION=$(git rev-parse HEAD)
make clean
mkdir -p ./bin

# by keeping all the required files in ./bin, it makes the build context
# for docker much smaller
cp ./src/query/config/m3coordinator-local-etcd.yml ./bin
cp ./src/dbnode/config/m3dbnode-local-etcd.yml ./bin

# build images
echo "building docker images"

for svc in ${SERVICES[@]}; do
  echo "creating image for $svc"
  make ${svc}-linux-amd64
  docker build -t "${svc}_integration:${REVISION}" -f ./scripts/docker-integration-tests/${svc}.Dockerfile ./bin
done
