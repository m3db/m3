#!/bin/bash

set -ex

git submodule update --init

echo $PATH
eval "$(.ci/gimme.sh 1.10.x)"
echo $PATH

which go

go get github.com/twitchtv/retool

ls -l /var/lib/buildkite-agent/.gimme/versions/go1.10.4.linux.amd64/bin

which retool

which make

make install-ci

make test-ci-unit