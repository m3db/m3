#!/bin/bash

# paranoia, ftw
set -ex

echo "Current PATH: $PATH"

echo "Checking if thrift is in PATH..."
if command -v thrift >/dev/null 2>&1; then
    echo "thrift found at: $(command -v thrift)"
else
    echo "thrift not found in PATH. Exiting."
    exit 1
fi

echo "Checking if thrift-gen is in PATH..."
if command -v thrift-gen >/dev/null 2>&1; then
    echo "thrift-gen found at: $(command -v thrift-gen)"
else
    echo "thrift-gen not found in PATH. Exiting."
    exit 1
fi

thrift-gen --generateThrift --inputFile rpc.thrift --outputDir ../rpc

# ensure docker is running
#docker run --rm hello-world >/dev/null

# generate files using dockerized thrift-gen
#THRIFT_IMAGE_VERSION=${THRIFT_IMAGE_VERSION:-"quay.io/m3db/thrift-gen:latest"}
#echo "Generating thrift files with image: $THRIFT_IMAGE_VERSION"

#UID_FLAGS="-u $(id -u)"
#if [[ -n "$BUILDKITE" ]]; then
#	UID_FLAGS="-u root"
#fi

#docker run --rm -v "$(pwd):/data" $UID_FLAGS    \
#  "$THRIFT_IMAGE_VERSION" --generateThrift      \
#  --inputFile /data/rpc.thrift --outputDir /data

# ensure formatting is correct
go fmt github.com/m3db/m3/src/dbnode/generated/thrift/rpc
