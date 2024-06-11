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

//go:generate sh -c "mockgen -package=fs $PACKAGE/src/dbnode/persist/fs DataFileSetWriter,DataFileSetReader,DataFileSetSeeker,IndexFileSetWriter,IndexFileSetReader,IndexSegmentFileSetWriter,IndexSegmentFileSet,IndexSegmentFile,SnapshotMetadataFileWriter,DataFileSetSeekerManager,ConcurrentDataFileSetSeeker,MergeWith,StreamingWriter | genclean -pkg $PACKAGE/src/dbnode/persist/fs -out ../../persist/fs/fs_mock.go"
//go:generate sh -c "mockgen -package=xio $PACKAGE/src/dbnode/x/xio ReaderSliceOfSlicesIterator,SegmentReader,SegmentReaderPool | genclean -pkg $PACKAGE/src/dbnode/x/xio -out ../../x/xio/io_mock.go"
//go:generate sh -c "mockgen -package=digest -destination=../../digest/digest_mock.go $PACKAGE/src/dbnode/digest ReaderWithDigest"
//go:generate sh -c "mockgen -package=series $PACKAGE/src/dbnode/storage/series DatabaseSeries,QueryableBlockRetriever | genclean -pkg $PACKAGE/src/dbnode/storage/series -out ../../storage/series/series_mock.go"
//go:generate sh -c "mockgen -package=storage $PACKAGE/src/dbnode/storage IndexWriter | genclean -pkg $PACKAGE/src/dbnode/storage -out ../../storage/lookup_mock.go"

// mockgen rules for generating mocks for unexported interfaces (file mode)
//go:generate sh -c "mockgen -package=encoding -destination=../../encoding/encoding_mock.go -source=../../encoding/types.go"
//go:generate sh -c "mockgen -package=bootstrap -destination=../../storage/bootstrap/bootstrap_mock.go -source=../../storage/bootstrap/types.go"
//go:generate sh -c "mockgen -package=persist -destination=../../persist/persist_mock.go -source=../../persist/types.go"
//go:generate sh -c "mockgen -package=storage -destination=../../storage/storage_mock.go -source=../../storage/types.go"
//go:generate sh -c "mockgen -package=series -destination=../../storage/series/buffer_mock.go -source=../../storage/series/buffer.go"
//go:generate sh -c "mockgen -package=block -destination=../../storage/block/block_mock.go -source=../../storage/block/types.go"
//go:generate sh -c "mockgen -package=rpc -destination=../../generated/thrift/rpc/rpc_mock.go -source=../../generated/thrift/rpc/tchan-rpc.go"
//go:generate sh -c "mockgen -package=client -destination=../../client/client_mock.go -source=../../client/types.go"
//go:generate sh -c "mockgen -package=commitlog -destination=../../persist/fs/commitlog/commit_log_mock.go -source=../../persist/fs/commitlog/types.go"
//go:generate sh -c "mockgen -package=topology -destination=../../topology/topology_mock.go -source=../../topology/types.go"
//go:generate sh -c "mockgen -package=retention -destination=../../retention/retention_mock.go -source=../../retention/types.go"
//go:generate sh -c "mockgen -package=namespace -destination=../../namespace/namespace_mock.go -source=../../namespace/types.go"
//go:generate sh -c "mockgen -package=kvadmin -destination=../../namespace/kvadmin/kvadmin_mock.go -source=../../namespace/kvadmin/types.go"
//go:generate sh -c "mockgen -package=runtime -destination=../../runtime/runtime_mock.go -source=../../runtime/types.go"
//go:generate sh -c "mockgen -package=writes -destination=../../ts/writes/write_batch_mock.go -source=../../ts/writes/types.go"
//go:generate sh -c "mockgen -package=index -destination=../../storage/index/index_mock.go -source=../../storage/index/types.go"
//go:generate sh -c "mockgen -package=permits -destination=../../storage/limits/permits/permits_mock.go -source=../../storage/limits/permits/types.go"
//go:generate sh -c "mockgen -package=xpool -destination=../../x/xpool/xpool_mock.go -source=../../x/xpool/types.go"

package mocks
