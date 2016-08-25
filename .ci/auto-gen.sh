#!/bin/bash
. "$(dirname $0)/variables.sh"

autogen_clear() {
    DIR="$1"

    rm -f ${DIR}/*
}

mocks_clear() {
    for DIR in $SRC;
    do
        MOCKS=${DIR}/*_mock.go
        if ls $MOCKS &> /dev/null; then
            for FILE in $(ls $MOCKS);
            do
                rm $FILE
            done
        fi
    done
}

autogen_cleanup() {
    DIR="$1"

    FILES=${DIR}/*.go
    for FILE in $(ls $FILES);
    do
        add_license $FILE $DIR
    done
}

mocks_cleanup() {
    for DIR in $SRC;
    do
        MOCKS=${DIR}/*_mock.go
        if ls $MOCKS &> /dev/null; then
            for FILE in $(ls $MOCKS);
            do
                add_license $FILE $DIR

                # NB(xichen): there is an open issue (https://github.com/golang/mock/issues/30)
                # with mockgen that causes the generated mock files to have vendored packages
                # in the import list. For now we are working around it by removing the vendored
                # path. Also sed -i'' does not work with BSD sed shipped with OS X, whereas
                # sed -i '' doesn't work with GNU sed, so we work around it by redirecting to a
                # temp file first and moving it back later.
                sed "s|$VENDOR_PATH/||" $FILE > $FILE.tmp && mv $FILE.tmp $FILE

                # Strip GOPATH from the source file path
                sed "s|Source: $GOPATH/src/\(.*\.go\)|Source: \1|" $FILE > $FILE.tmp && mv $FILE.tmp $FILE
            done
        fi
    done
}

add_license() {
    FILE="$1"
    DIR="$2"

    # Add uber license
    PREV_PWD=$(pwd)
    cd $DIR
    $LICENSE_BIN --silent --file $(basename $FILE)
    cd $PREV_PWD
}

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: auto-gen.sh output_directory file_generation_rules_directory"
    exit 1
fi

set -e

. "$(dirname $0)/variables.sh"

if [ "$2" = "generated/mocks" ]; then
    mocks_clear
else
    autogen_clear $1
fi

go generate $PACKAGE/$2

if [ "$2" = "generated/mocks" ]; then
    mocks_cleanup
else
    autogen_cleanup $1
fi
