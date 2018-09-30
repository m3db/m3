#!/bin/bash

set -ex

git submodule update --init

eval "$(.ci/gimme.sh 1.10.x)"

make install-ci

make test-ci-unit