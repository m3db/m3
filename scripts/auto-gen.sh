#!/bin/bash
source "$(dirname $0)/../.ci/variables.sh"

set -e

add_license() {
    local FILE="$1"
    update-license $FILE
}

export -f add_license

autogen_clear() {
	local DIR="$1"
    find $DIR -mindepth 2 -type f -name '*.go' -exec rm -f {} \;
}

remove_matching_files() {
    local FILE_PATTERN=$1
    for DIR in $SRC;
    do
        find $DIR -type f -name "$FILE_PATTERN" -exec rm -f {} \;
    done
}

autogen_cleanup() {
    local DIR="$1"
    find $DIR -type f -name "*.go" -exec /bin/bash -c 'add_license $0' {} \;
}

mocks_cleanup_helper() {
    local FILE=$0
    local DIR=$(dirname $FILE)
    add_license $FILE

    # NB(xichen): there is an open issue (https://github.com/golang/mock/issues/30)
    # with mockgen that causes the generated mock files to have vendored packages
    # in the import list. For now we are working around it by removing the vendored
    # path. Also sed -i'' does not work with BSD sed shipped with OS X, whereas
    # sed -i '' doesn't work with GNU sed, so we work around it by redirecting to a
    # temp file first and moving it back later.
    sed "s|$VENDOR_PATH/||" $FILE > $FILE.tmp && mv $FILE.tmp $FILE

    # Strip GOPATH from the source file path
    sed "s|Source: $GOPATH/src/\(.*\.go\)|Source: \1|" $FILE > $FILE.tmp && mv $FILE.tmp $FILE

    # NB(prateek): running mockclean makes mock-gen idempotent.
    # NB(xichen): mockclean should be run after the vendor path is stripped.
    basePkg=$(echo $DIR | sed -e "s@${GOPATH}/src/@@g")
    mockclean -pkg $basePkg -out $FILE -in $FILE
    gofmt -w $FILE
}

export -f mocks_cleanup_helper

mocks_cleanup() {
    local MOCK_PATTERN=$1
    for DIR in $SRC;
    do
        find $DIR -name "$MOCK_PATTERN" -type f -exec /bin/bash -c 'mocks_cleanup_helper $0' {} \;
    done
}

generics_cleanup() {
    local GEN_FILES_PATTERN=$1
    for DIR in $SRC;
    do
        find $DIR -name "$GEN_FILES_PATTERN" -type f -exec /bin/bash -c 'add_license $0' {} \;
    done
}

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: auto-gen.sh output_directory file_generation_rules_directory"
    exit 1
fi

if [[ "$2" = *"generated/mocks"* ]]; then
    remove_matching_files "*_mock.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    remove_matching_files "*.gen.go"
else
    autogen_clear $1
fi

go generate $PACKAGE/$2

if [[ "$2" = *"generated/mocks"* ]]; then
    mocks_cleanup "*_mock.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    generics_cleanup "*.gen.go"
else
    autogen_cleanup $1
fi
