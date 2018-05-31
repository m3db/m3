#!/bin/sh

ASSET_SRC=$1
for i in "$ASSET_SRC"/*; do
    if ! [ -d $i ]; then
        continue
    fi

    echo "generating from $i"
    esc -prefix "${i##*/}/" -pkg "${i##*/}" -ignore .go -o "${i##*/}/assets.go" "."
done
