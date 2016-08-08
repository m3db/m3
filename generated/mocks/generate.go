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
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/block.go $PACKAGE/interfaces/m3db DatabaseBlock,DatabaseSeriesBlocks"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/bootstrap.go $PACKAGE/interfaces/m3db ShardResult,Bootstrap,Bootstrapper,Source"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/client.go $PACKAGE/interfaces/m3db Client"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/encoding.go $PACKAGE/interfaces/m3db Encoder,Iterator,ReaderIterator,MultiReaderIterator,Decoder"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/fs.go $PACKAGE/interfaces/m3db FileSetWriter,FileSetReader"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/commit_log.go $PACKAGE/interfaces/m3db CommitLog,CommitLogIterator"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/options.go $PACKAGE/interfaces/m3db DatabaseOptions,ClientOptions,TopologyTypeOptions"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/persist.go $PACKAGE/interfaces/m3db PersistenceManager"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/reader.go $PACKAGE/interfaces/m3db ReaderSliceReader,SegmentReader"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/series_iterator.go $PACKAGE/interfaces/m3db SeriesIterator"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/series_iterators.go $PACKAGE/interfaces/m3db SeriesIterators"
//go:generate sh -c "mockgen -package=mocks -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/tchan_rpc.go $PACKAGE/generated/thrift/rpc TChanCluster,TChanNode"

// mockgen rules for generating mocks for unexported interfaces (file mode)
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/client/connection_pool.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/connection_pool.go "
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/client/host_queue.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/host_queue.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/client/session.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/session.go -aux_files=m3db=$GOPATH/src/$PACKAGE/interfaces/m3db/client.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/storage/buffer.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/buffer.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/storage/series.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/series.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/storage/shard.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/shard.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/persist/fs/commitlog/writer.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/commit_log_writer.go"
//go:generate sh -c "mockgen -package=mocks -source=$GOPATH/src/$PACKAGE/persist/fs/commitlog/reader.go -destination=$GOPATH/src/$PACKAGE/generated/mocks/mocks/commit_log_reader.go"

package mocks
