#!/bin/sh

set -x

git submodule update --init

which go

make install-ci

make test-ci-big-unit