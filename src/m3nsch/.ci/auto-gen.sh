#!/bin/bash
set -x
. "$(dirname $0)/variables.sh"

autogen_clear() {
	DIR="$1"

	FILES=${DIR}/*
	for FILE in $(ls $FILES); do
		if [ -d $FILE ]; then
			autogen_subdir_clear $FILE
		fi
	done
}

autogen_subdir_clear() {
    DIR="$1"

    rm -f ${DIR}/*.go
}

mocks_clear() {
    local MOCK_PATTERN=$1
    for DIR in $SRC;
    do
        local MOCKS=${DIR}/$MOCK_PATTERN
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

    FILES=${DIR}/*
    for FILE in $(ls $FILES);
    do
      if [ -d $FILE ]; then
        autogen_cleanup $FILE
      else
        add_license $FILE $DIR
      fi
    done
}

mocks_cleanup() {
    local MOCK_PATTERN=$1
    for DIR in $SRC;
    do
        local MOCKS=${DIR}/${MOCK_PATTERN}
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
    # NOTE: m3db uses the *_mock.go convention, m3cluster uses mock_*.go
    mocks_clear "*_mock.go"
    mocks_clear "mock_*.go"
else
    autogen_clear $1
fi

go generate $PACKAGE/$2

if [ "$2" = "generated/mocks" ]; then
    # NOTE: m3db uses the *_mock.go convention, m3cluster uses mock_*.go
    mocks_cleanup "*_mock.go"
    mocks_cleanup "mock_*.go"
else
    autogen_cleanup $1
fi
