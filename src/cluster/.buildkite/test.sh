#!/bin/bash

set -ex

git submodule update --init --recursive
make install-ci
TEST_TIMEOUT_SCALE=20 PACKAGE=github.com/m3db/m3cluster make all
