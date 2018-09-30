#!/bin/bash

set -x

git submodule update --init

export CGO_ENABLED=0
echo $PATH
eval "$(.ci/gimme.sh 1.10.x)"
echo $PATH

which go

go get -u github.com/twitchtv/retool

make install-ci

make test-ci-big-unit