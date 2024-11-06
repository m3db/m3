#!/bin/bash

# include poratable readlink
source $(dirname $0)/realpath.sh

# paranoia, ftw
set -ex

PROTO_SRC=$1
for i in "${GOPATH}/src/${PROTO_SRC}"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then
		proto_files=$(ls $i/*.proto | sed -e "s@${GOPATH}@${GOPATH}@g")
		echo "generating from ${proto_files}"
    resolve_protos="Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types"

	  protoc --gogofaster_out=${resolve_protos},plugins=grpc:${GOPATH}/src \
      -I${GOPATH}/src -I${GOPATH}/src/github.com/m3db/m3/vendor ${proto_files}
	fi
done
