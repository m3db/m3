#!/bin/bash

# paranoia, ftw
set -e

# ensure docker is running
docker run --rm hello-world >/dev/null

# generate files using dockerized thrift-gen
THRIFT_IMAGE_VERSION=${THRIFT_IMAGE_VERSION:-"quay.io/m3db/thrift-gen:0.2.0"}
echo "Generating thrift files with image: $THRIFT_IMAGE_VERSION"

UID_FLAGS="-u $(id -u)"
if [[ -n "$BUILDKITE" ]]; then
	UID_FLAGS="-u root"
fi

docker run --rm -v "$(pwd):/data" $UID_FLAGS    \
  "$THRIFT_IMAGE_VERSION" --generateThrift      \
  --inputFile /data/rpc.thrift --outputDir /data

# ensure formatting is correct
go fmt github.com/m3db/m3/src/dbnode/generated/thrift/rpc
