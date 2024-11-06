#!/bin/bash

# paranoia, ftw
set -ex

thrift --gen go rpc.thrift

# ensure formatting is correct
go fmt github.com/m3db/m3/src/dbnode/generated/thrift/rpc
