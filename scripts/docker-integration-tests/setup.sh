#!/usr/bin/env bash

set -xe

# expected to be run from root of repository
cd $GOPATH/src/github.com/m3db/m3

SERVICES=(m3dbnode m3coordinator m3aggregator)
REVISION=$(git rev-parse HEAD)
if [[ $SKIP_CLEAN != "true" ]]; then
  make clean
fi
mkdir -p ./bin

# by keeping all the required files in ./bin, it makes the build context
# for docker much smaller
cp ./src/query/config/m3coordinator-local-etcd.yml ./bin
cp ./src/dbnode/config/m3dbnode-local-etcd.yml ./bin
cp ./src/aggregator/config/m3aggregator.yml ./bin

# build images
echo "building docker images"

for svc in ${SERVICES[@]}; do
  # only build if image doesn't exist
  if [[ "$(docker images -q ${svc}_integration:${REVISION} 2> /dev/null)" == "" ]]; then
    echo "creating image for $svc"
    make ${svc}-linux-amd64
    docker build -t "${svc}_integration:${REVISION}" -f ./scripts/docker-integration-tests/${svc}.Dockerfile ./bin
  fi
done
