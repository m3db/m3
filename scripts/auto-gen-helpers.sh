#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ROOT="${DIR}/.."

source "${ROOT}/.ci/variables.sh"


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

revert_copyright_only_change() {
    # We don't want to make a change to a file if the only change
    # is the copyright year. We can't check this in add_license because a newly-
    # generated file will not contain the copyright notice and thus it will
    # add in the copyright (with the new year).
    local FILE=$0
    numDiffLines=$(git --no-pager diff --text -U0 $FILE | # Get file text diffs with no context.
        grep -E -v '^\+\+\+|^---'                       | # Exclude file descriptors.
        grep -E '^-|^\+'                                | # Get only line diffs.
        grep -Evc '^-// Copyright \(c\)|^\+// Copyright \(c\)') # Exclude copyrights and get the number of lines remaining.
    if [ $numDiffLines = 0 ]; then
        git checkout -- "$FILE" 2> /dev/null # Remove changes, since the only change was the copyright year.
    fi
}

export -f revert_copyright_only_change

autogen_cleanup() {
    local DIR="$1"
    find $DIR -type f -name "*.go" -exec /bin/bash -c 'add_license $0; revert_copyright_only_change $0' {} \;
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
    revert_copyright_only_change $FILE
}

export -f gen_cleanup_helper

gen_cleanup_dir() {
    local PATTERN=$1
    local DIRS=$2
    for DIR in $DIRS;
    do
        find $DIR -name "$PATTERN" -type f -exec /bin/bash -c 'gen_cleanup_helper $0' {} \;
    done
}

gen_cleanup() {
    gen_cleanup_dir $1 $SRC
}
