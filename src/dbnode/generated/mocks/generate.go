// Copyright (c) 2018 Uber Technologies, Inc.
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

//go:generate sh -c "mockgen -package=fs $PACKAGE/src/dbnode/persist/fs DataFileSetWriter,DataFileSetReader,DataFileSetSeeker,IndexFileSetWriter,IndexFileSetReader,IndexSegmentFileSetWriter,IndexSegmentFileSet,IndexSegmentFile,SnapshotMetadataFileWriter,DataFileSetSeekerManager,ConcurrentDataFileSetSeeker,MergeWith,StreamingWriter,DataEntryProcessor | genclean -pkg $PACKAGE/src/dbnode/persist/fs -out $GOPATH/src/$PACKAGE/src/dbnode/persist/fs/fs_mock.go"
//go:generate sh -c "mockgen -package=xio $PACKAGE/src/dbnode/x/xio SegmentReader,SegmentReaderPool | genclean -pkg $PACKAGE/src/dbnode/x/xio -out $GOPATH/src/$PACKAGE/src/dbnode/x/xio/io_mock.go"
//go:generate sh -c "mockgen -package=digest -destination=$GOPATH/src/$PACKAGE/src/dbnode/digest/digest_mock.go $PACKAGE/src/dbnode/digest ReaderWithDigest"
//go:generate sh -c "mockgen -package=series $PACKAGE/src/dbnode/storage/series DatabaseSeries,QueryableBlockRetriever | genclean -pkg $PACKAGE/src/dbnode/storage/series -out $GOPATH/src/$PACKAGE/src/dbnode/storage/series/series_mock.go"
//go:generate sh -c "mockgen -package=lookup $PACKAGE/src/dbnode/storage/series/lookup OnReleaseReadWriteRef,IndexWriter | genclean -pkg $PACKAGE/src/dbnode/storage/series/lookup -out $GOPATH/src/$PACKAGE/src/dbnode/storage/series/lookup/lookup_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode)
//go:generate sh -c "mockgen -package=encoding -destination=$GOPATH/src/$PACKAGE/src/dbnode/encoding/encoding_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/encoding/types.go"
//go:generate sh -c "mockgen -package=bootstrap -destination=$GOPATH/src/$PACKAGE/src/dbnode/storage/bootstrap/bootstrap_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/storage/bootstrap/types.go"
//go:generate sh -c "mockgen -package=persist -destination=$GOPATH/src/$PACKAGE/src/dbnode/persist/persist_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/persist/types.go"
//go:generate sh -c "mockgen -package=storage -destination=$GOPATH/src/$PACKAGE/src/dbnode/storage/storage_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/storage/types.go"
//go:generate sh -c "mockgen -package=series -destination=$GOPATH/src/$PACKAGE/src/dbnode/storage/series/buffer_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/storage/series/buffer.go"
//go:generate sh -c "mockgen -package=block -destination=$GOPATH/src/$PACKAGE/src/dbnode/storage/block/block_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/storage/block/types.go"
//go:generate sh -c "mockgen -package=rpc -destination=$GOPATH/src/$PACKAGE/src/dbnode/generated/thrift/rpc/rpc_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/generated/thrift/rpc/tchan-rpc.go"
//go:generate sh -c "mockgen -package=client -destination=$GOPATH/src/$PACKAGE/src/dbnode/client/client_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/client/types.go"
//go:generate sh -c "mockgen -package=commitlog -destination=$GOPATH/src/$PACKAGE/src/dbnode/persist/fs/commitlog/commit_log_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/persist/fs/commitlog/types.go"
//go:generate sh -c "mockgen -package=topology -destination=$GOPATH/src/$PACKAGE/src/dbnode/topology/topology_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/topology/types.go"
//go:generate sh -c "mockgen -package=retention -destination=$GOPATH/src/$PACKAGE/src/dbnode/retention/retention_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/retention/types.go"
//go:generate sh -c "mockgen -package=namespace -destination=$GOPATH/src/$PACKAGE/src/dbnode/namespace/namespace_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/namespace/types.go"
//go:generate sh -c "mockgen -package=kvadmin -destination=$GOPATH/src/$PACKAGE/src/dbnode/namespace/kvadmin/kvadmin_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/namespace/kvadmin/types.go"
//go:generate sh -c "mockgen -package=runtime -destination=$GOPATH/src/$PACKAGE/src/dbnode/runtime/runtime_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/runtime/types.go"
//go:generate sh -c "mockgen -package=writes -destination=$GOPATH/src/$PACKAGE/src/dbnode/ts/writes/write_batch_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/ts/writes/types.go"
//go:generate sh -c "mockgen -package=index -destination=$GOPATH/src/$PACKAGE/src/dbnode/storage/index/index_mock.go -source=$GOPATH/src/$PACKAGE/src/dbnode/storage/index/types.go"

package mocks
