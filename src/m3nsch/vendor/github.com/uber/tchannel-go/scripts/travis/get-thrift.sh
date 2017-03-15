#!/bin/bash

set -e

cd "$(dirname "$0")"
rm -rf thrift-release.zip
wget https://github.com/prashantv/thrift/releases/download/p0.0.1/thrift-release.zip
unzip thrift-release.zip

