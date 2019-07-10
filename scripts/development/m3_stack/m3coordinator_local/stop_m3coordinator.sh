#!/usr/bin/env bash

set -xe

echo "Stopping coordinator"
killall m3coordinator
