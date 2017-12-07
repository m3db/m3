#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
DIR=$1
pushd ${DIR}
protoc --gofast_out=plugins=grpc:. --proto_path=.:$GOPATH/src/github.com/gogo/protobuf *.proto
popd
