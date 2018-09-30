#!/bin/bash

set -x

git submodule update --init

export CGO_ENABLED=0
echo $PATH
eval "$(.ci/gimme.sh 1.10.x)"
echo $PATH

which go

go get -u github.com/twitchtv/retool

ls -l /var/lib/buildkite-agent/.gimme/versions/go1.10.4.linux.amd64/bin

which retool

which make

make install-ci

make test-ci-big-unit