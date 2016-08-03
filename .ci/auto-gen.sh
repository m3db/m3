#!/bin/bash
 
cleanup() {
    OLD_PATH=$PWD
    cd $1
    for FILE in $(ls *.go);
    do
        # NB(xichen): there is an open issue (https://github.com/golang/mock/issues/30)
        # with mockgen that causes the generated mock files to have vendored packages
        # in the import list. For now we are working around it by removing the vendored
        # path. Also sed -i'' does not work with BSD sed shipped with OS X, whereas
        # sed -i '' doesn't work with GNU sed, so we work around it by redirecting to a
        # temp file first and moving it back later.
        sed "s|$VENDOR_PATH/||" $FILE > $FILE.tmp && mv $FILE.tmp $FILE
        
        # Add uber license
        $LICENSE_BIN --silent --file $FILE
    done
    cd $OLD_PATH
}

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: auto-gen.sh output_directory file_generation_rules_directory"
    exit 1
fi

set -e

. "$(dirname $0)/variables.sh"

rm -f $1/*
go generate $PACKAGE/$2
cleanup $1
