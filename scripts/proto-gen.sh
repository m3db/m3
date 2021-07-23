#!/bin/bash

# include poratable readlink
source $(dirname $0)/realpath.sh

# paranoia, ftw
set -e

PROTOC_IMAGE_VERSION=${PROTOC_IMAGE_VERSION:-"znly/protoc:0.2.0"}

# ensure docker is running
docker run --rm hello-world >/dev/null

UID_FLAGS="-u $(id -u)"
if [[ -n "$BUILDKITE" ]]; then
	UID_FLAGS="-u root"
fi

PROTO_SRC=$1
for i in "${GOPATH}/src/${PROTO_SRC}"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then
		proto_files=$(ls $i/*.proto | sed -e "s@${GOPATH}@@g")
		echo "generating from ${proto_files}"
		# need the additional m3db_path mount in docker because it's a symlink on the CI.
		m3db_path=$(realpath $GOPATH/src/github.com/m3db/m3)
    resolve_protos="Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types"

		docker run --rm -w /src -v $GOPATH/src:/src -v ${m3db_path}:/src/github.com/m3db/m3 \
		$UID_FLAGS $PROTOC_IMAGE_VERSION \
			 --gogofaster_out=${resolve_protos},plugins=grpc:/src \
			 -I/src -I/src/github.com/m3db/m3/vendor ${proto_files}
	fi
done
