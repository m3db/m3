#!/bin/bash

export PACKAGE='github.com/m3db/m3db'
export VENDOR_PATH=$PACKAGE/vendor
export LICENSE_BIN=$GOPATH/src/$VENDOR_PATH/github.com/uber/uber-licence/bin/licence
export GO15VENDOREXPERIMENT=1
