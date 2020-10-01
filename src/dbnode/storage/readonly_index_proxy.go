// Copyright (c) 2020 Uber Technologies, Inc.
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

package storage

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var errNamespaceIndexReadOnly = errors.New("write operation on read only namespace index")

type readOnlyIndexProxy struct {
	underlying NamespaceIndex
}

func (r readOnlyIndexProxy) AssignShardSet(shardSet sharding.ShardSet) {}

func (r readOnlyIndexProxy) BlockStartForWriteTime(writeTime time.Time) xtime.UnixNano {
	return r.underlying.BlockStartForWriteTime(writeTime)
}

func (r readOnlyIndexProxy) BlockForBlockStart(blockStart time.Time) (index.Block, error) {
	return r.underlying.BlockForBlockStart(blockStart)
}

func (r readOnlyIndexProxy) WriteBatch(batch *index.WriteBatch) error {
	return errNamespaceIndexReadOnly
}

func (r readOnlyIndexProxy) WritePending(pending []writes.PendingIndexInsert) error {
	return errNamespaceIndexReadOnly
}

func (r readOnlyIndexProxy) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResult, error) {
	return r.underlying.Query(ctx, query, opts)
}

func (r readOnlyIndexProxy) AggregateQuery(
	ctx context.Context,
	query index.Query,
	opts index.AggregationOptions,
) (index.AggregateQueryResult, error) {
	return r.underlying.AggregateQuery(ctx, query, opts)
}

func (r readOnlyIndexProxy) WideQuery(
	ctx context.Context,
	query index.Query,
	collector chan *ident.IDBatch,
	opts index.WideQueryOptions,
) error {
	return r.underlying.WideQuery(ctx, query, collector, opts)
}

func (r readOnlyIndexProxy) Bootstrap(bootstrapResults result.IndexResults) error {
	return nil
}

func (r readOnlyIndexProxy) Bootstrapped() bool {
	return r.underlying.Bootstrapped()
}

func (r readOnlyIndexProxy) CleanupExpiredFileSets(t time.Time) error {
	return nil
}

func (r readOnlyIndexProxy) CleanupDuplicateFileSets() error {
	return nil
}

func (r readOnlyIndexProxy) Tick(c context.Cancellable, startTime time.Time) (namespaceIndexTickResult, error) {
	return namespaceIndexTickResult{}, nil
}

func (r readOnlyIndexProxy) WarmFlush(flush persist.IndexFlush, shards []databaseShard) ([]time.Time, error) {
	return nil, nil
}

func (r readOnlyIndexProxy) ColdFlush(shards []databaseShard) (OnColdFlushDone, error) {
	return noopOnColdFlushDone, nil
}

func (r readOnlyIndexProxy) SetExtendedRetentionPeriod(period time.Duration) {
	r.underlying.SetExtendedRetentionPeriod(period)
}

func (r readOnlyIndexProxy) DebugMemorySegments(opts DebugMemorySegmentsOptions) error {
	return r.underlying.DebugMemorySegments(opts)
}

func (r readOnlyIndexProxy) BlockStatesSnapshot() index.BlockStateSnapshot {
	return index.NewBlockStateSnapshot(false, index.BootstrappedBlockStateSnapshot{})
}

func (r readOnlyIndexProxy) SetSnapshotStateVersionFlushed(blockStart time.Time, version int) {}

func (r readOnlyIndexProxy) Snapshot(
	shards map[uint32]struct{},
	blockStart,
	snapshotTime time.Time,
	snapshotPersist persist.SnapshotPreparer,
	infoFiles []fs.ReadIndexInfoFileResult,
) error {
	return nil
}

func (r readOnlyIndexProxy) Close() error {
	return nil
}

// NewReadOnlyIndexProxy builds a new NamespaceIndex that proxies only read
// operations, and no-ops on write operations.
func NewReadOnlyIndexProxy(underlying NamespaceIndex) NamespaceIndex {
	return readOnlyIndexProxy{underlying: underlying}
}

func noopOnColdFlushDone() error {
	return nil
}
