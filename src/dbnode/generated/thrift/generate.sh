#!/bin/bash

# paranoia, ftw
set -e

# ensure docker is running
docker run --rm hello-world >/dev/null

# generate files using dockerized thrift-gen
THRIFT_IMAGE_VERSION=${THRIFT_IMAGE_VERSION:-"quay.io/m3db/thrift-gen:0.1.0"}
echo "Generating thrift files with image: $THRIFT_IMAGE_VERSION"

UID_FLAGS="-u $(id -u)"
if [[ -n "$BUILDKITE" ]]; then
	UID_FLAGS="-u root"
fi

# move any extensions so they don't get clobbered by the generate
EXTENSIONS_FILE="extensions.go"
if [ -f "rpc/$EXTENSIONS_FILE" ]; then
  mv "rpc/$EXTENSIONS_FILE" .
fi

docker run --rm -v "$(pwd):/data" $UID_FLAGS    \
  "$THRIFT_IMAGE_VERSION" --generateThrift      \
  --inputFile /data/rpc.thrift --outputDir /data

# replace extensions
if [ -f "$EXTENSIONS_FILE" ]; then
  mv "$EXTENSIONS_FILE" "rpc/"
fi

# rename handleFetchTagged method to allow for custom impl
sed -i.bak "s/ handleFetchTagged/ __handleFetchTagged/" rpc/tchan-rpc.go
# remove back up file (required to create to make in-place edit work on OS X)
rm rpc/tchan-rpc.go.bak

# ensure formatting is correct
go fmt github.com/m3db/m3/src/dbnode/generated/thrift/rpc
