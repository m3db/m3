export PACKAGE='github.com/m3db/m3cluster'
export GO15VENDOREXPERIMENT=1
export VENDOR_PATH=$PACKAGE/vendor
export LICENSE_BIN=$GOPATH/src/$VENDOR_PATH/github.com/uber/uber-licence/bin/licence
export SRC=$(find ./ -maxdepth 10 -not -path '*/.git*' -not -path '*/.ci*' -not -path '*/_*' -not -path '*/vendor/*' -type d)
