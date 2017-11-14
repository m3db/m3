#!/usr/bin/env bash
#
# Generate all protobuf bindings.
# Run from repository root.
set -e
set -u

if ! [[ "$0" =~ "scripts/genproto.sh" ]]; then
	echo "must be run from repository root"
	exit 255
fi

if ! [[ $(protoc --version) =~ "3.4.0" ]]; then
	echo "could not find protoc 3.4.0, is it installed + in PATH?"
	exit 255
fi

DIR="prometheus/prompb"
pushd ${DIR}
protoc --gofast_out=plugins=grpc:. --proto_path=.:$GOPATH/src/github.com/gogo/protobuf *.proto
popd
