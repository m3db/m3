#!/usr/bin/env bash

PROTO_SRC=$1
for i in "$PROTO_SRC"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then
		echo "generating from $i"
		protoc --gofast_out=plugins=grpc:$i --proto_path=$i:$GOPATH/src/github.com/m3db/m3coordinator/vendor $i/*.proto
	fi
done
