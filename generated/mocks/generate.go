// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// mockgen rules for generating mocks for exported interfaces (reflection mode)
//go:generate sh -c "mockgen -package=fs -destination=$GOPATH/src/$PACKAGE/persist/fs/fs_mock.go $PACKAGE/persist/fs FileSetWriter,FileSetReader"
//go:generate sh -c "mockgen -package=xio -destination=$GOPATH/src/$PACKAGE/x/io/io_mock.go $PACKAGE/x/io ReaderSliceReader,SegmentReader"
//go:generate sh -c "mockgen -package=series -destination=$GOPATH/src/$PACKAGE/storage/series/series_mock.go $PACKAGE/storage/series DatabaseSeries"

// mockgen rules for generating mocks for unexported interfaces (file mode)
//go:generate sh -c "mockgen -package=encoding -destination=$GOPATH/src/$PACKAGE/encoding/encoding_mock.go -source=$GOPATH/src/$PACKAGE/encoding/types.go"
//go:generate sh -c "mockgen -package=bootstrap -destination=$GOPATH/src/$PACKAGE/storage/bootstrap/bootstrap_mock.go -source=$GOPATH/src/$PACKAGE/storage/bootstrap/types.go"
//go:generate sh -c "mockgen -package=persist -destination=$GOPATH/src/$PACKAGE/persist/persist_mock.go -source=$GOPATH/src/$PACKAGE/persist/types.go"
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/storage/storage_mock.go -source=$GOPATH/src/$PACKAGE/storage/types.go"
//go:generate sh -c "mockgen -package=series -destination=$GOPATH/src/$PACKAGE/storage/series/buffer_mock.go -source=$GOPATH/src/$PACKAGE/storage/series/buffer.go"
//go:generate sh -c "mockgen -package=block -destination=$GOPATH/src/$PACKAGE/storage/block/block_mock.go -source=$GOPATH/src/$PACKAGE/storage/block/types.go"
//go:generate sh -c "mockgen -package=rpc -destination=$GOPATH/src/$PACKAGE/generated/thrift/rpc/rpc_mock.go -source=$GOPATH/src/$PACKAGE/generated/thrift/rpc/tchan-rpc.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/$PACKAGE/client/client_mock.go -source=$GOPATH/src/$PACKAGE/client/types.go"
//go:generate sh -c "mockgen -package=commitlog -destination=$GOPATH/src/$PACKAGE/persist/fs/commitlog/commit_log_mock.go -source=$GOPATH/src/$PACKAGE/persist/fs/commitlog/types.go"
//go:generate sh -c "mockgen -package=topology -destination=$GOPATH/src/$PACKAGE/topology/topology_mock.go -source=$GOPATH/src/$PACKAGE/topology/types.go"

package mocks
