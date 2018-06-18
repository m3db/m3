#!/bin/bash

# include poratable readlink
source $(dirname $0)/realpath.sh

# paranoia, ftw
set -e

PROTOC_IMAGE_VERSION=${PROTOC_IMAGE_VERSION:-"znly/protoc:0.2.0"}

# ensure docker is running
docker run --rm hello-world >/dev/null

PROTO_SRC=$1
for i in "${GOPATH}/src/${PROTO_SRC}"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then
		proto_files=$(ls $i/*.proto | sed -e "s@${GOPATH}@@g")
		echo "generating from ${proto_files}"
		# need the additional m3db_path mount in docker because it's a symlink on the CI.
		m3db_path=$(realpath $GOPATH/src/github.com/m3db/m3db)
		docker run --rm -w /src -v $GOPATH/src:/src -v ${m3db_path}:/src/github.com/m3db/m3db \
	   	$PROTOC_IMAGE_VERSION --gogofaster_out=plugins=grpc:/src                            \
			 -I/src -I/src/github.com/m3db/m3db/vendor ${proto_files}
	fi
done
