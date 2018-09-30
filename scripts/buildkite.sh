#!/bin/bash

set -ex

git submodule update --init

eval "$(.ci/gimme.sh 1.10.x)"

which go

go get github.com/twitchtv/retool

which make

make install-ci

make test-ci-unit