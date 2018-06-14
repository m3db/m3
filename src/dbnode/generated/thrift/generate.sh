#!/bin/bash

# paranoia, ftw
set -e

# ensure docker is running
docker run --rm hello-world >/dev/null

# generate files using dockerized thrift-gen
THRIFT_IMAGE_VERSION=${THRIFT_IMAGE_VERSION:-"quay.io/prateekrungta/thrift-gen:0.1.0"}
docker run --rm -u $(id -u) -v "$(pwd):/data" \
  "$THRIFT_IMAGE_VERSION" --generateThrift    \
  --inputFile /data/rpc.thrift --outputDir /data

# ensure formatting is correct
go fmt github.com/m3db/m3db/src/dbnode/generated/thrift/rpc
