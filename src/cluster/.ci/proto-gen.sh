#!/bin/sh

PROTO_SRC=$1
for i in "$PROTO_SRC"/*; do
	if ! [ -d $i ]; then
		continue
	fi

	if ls $i/*.proto > /dev/null 2>&1; then
			echo "generating from $i"
			protoc -I$i --go_out=$i $i/*.proto
	fi
done


