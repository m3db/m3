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
    local DIR=$1
    local FILE_PATTERN=$2
    find $DIR -type f -name "$FILE_PATTERN" -exec rm -f {} \;
}

autogen_cleanup() {
    local DIR="$1"
    find $DIR -type f -name "*.go" -exec /bin/bash -c 'add_license $0' {} \;
}

gen_cleanup_helper() {
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

    # NB(prateek): running genclean makes mock-gen idempotent.
    # NB(xichen): genclean should be run after the vendor path is stripped.
    basePkg=$(echo $DIR | sed -e "s@${GOPATH}/src/@@g")
    genclean -pkg $basePkg -out $FILE -in $FILE
    gofmt -w $FILE
}

export -f gen_cleanup_helper

gen_cleanup() {
    local PATTERN=$1
    for DIR in $SRC;
    do
        find $DIR -name "$PATTERN" -type f -exec /bin/bash -c 'gen_cleanup_helper $0' {} \;
    done
}

if [ $# -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
    echo "usage: auto-gen.sh output_directory file_generation_rules_directory"
    exit 1
fi

if [[ "$2" = *"generated/mocks"* ]]; then
    remove_matching_files $1 "*_mock.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    remove_matching_files $1 "*.gen.go"
else
    autogen_clear $1
fi

go generate $PACKAGE/$2

if [[ "$2" = *"generated/mocks"* ]]; then
    gen_cleanup "*_mock.go"
elif [[ "$2" = *"generated/generics"* ]]; then
    gen_cleanup "*.gen.go"
else
    autogen_cleanup $1
fi
