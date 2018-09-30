#!/bin/sh

set -ex

git submodule update --init

which go

make install-ci

make test-ci-integration-dbnode cache_policy=all_metadata

# make test-ci-big-unit