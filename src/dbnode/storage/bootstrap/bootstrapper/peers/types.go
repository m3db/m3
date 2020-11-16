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

package peers

import (
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/x/context"
)

// Options represents the options for bootstrapping from peers.
type Options interface {
	// Validate validates the options.
	Validate() error

	// SetResultOptions sets the instrumentation options.
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the instrumentation options.
	ResultOptions() result.Options

	// SetAdminClient sets the admin client.
	SetAdminClient(value client.AdminClient) Options

	// AdminClient returns the admin client.
	AdminClient() client.AdminClient

	// SetDefaultShardConcurrency sets the concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	SetDefaultShardConcurrency(value int) Options

	// DefaultShardConcurrency returns the concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	DefaultShardConcurrency() int

	// SetShardPersistenceConcurrency sets the concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	SetShardPersistenceConcurrency(value int) Options

	// ShardPersistenceConcurrency returns the concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	ShardPersistenceConcurrency() int

	// SetShardPersistenceFlushConcurrency sets the flush concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	SetShardPersistenceFlushConcurrency(value int) Options

	// ShardPersistenceFlushConcurrency returns the flush concurrency for
	// bootstrapping shards when performing a bootstrap with
	// persistence enabled.
	ShardPersistenceFlushConcurrency() int

	// SetIndexSegmentConcurrency sets the concurrency for
	// building index segments.
	SetIndexSegmentConcurrency(value int) Options

	// IndexSegmentConcurrency returns the concurrency for
	// building index segments.
	IndexSegmentConcurrency() int

	// SetPersistenceMaxQueueSize sets the max queue for
	// bootstrapping shards waiting in line to persist without blocking
	// the concurrent shard fetchers.
	SetPersistenceMaxQueueSize(value int) Options

	// PersistenceMaxQueueSize returns the max queue for
	// bootstrapping shards waiting in line to persist without blocking
	// the concurrent shard fetchers.
	PersistenceMaxQueueSize() int

	// SetPersistManager sets the persistence manager used to flush blocks
	// when performing a bootstrap with persistence.
	SetPersistManager(value persist.Manager) Options

	// PersistManager returns the persistence manager used to flush blocks
	// when performing a bootstrap with persistence.
	PersistManager() persist.Manager

	// SetIndexClaimsManager sets the index claims manager.
	SetIndexClaimsManager(value fs.IndexClaimsManager) Options

	// IndexClaimsManager returns the index claims manager. It's used to manage
	// concurrent claims for volume indices per ns and block start.
	IndexClaimsManager() fs.IndexClaimsManager

	// SetCompactor sets the compactor used to compact segment builders into segments.
	SetCompactor(value *compaction.Compactor) Options

	// Compactor returns the compactor used to compact segment builders into segments.
	Compactor() *compaction.Compactor

	// SetRuntimeOptionsManagers sets the RuntimeOptionsManager.
	SetRuntimeOptionsManager(value m3dbruntime.OptionsManager) Options

	// RuntimeOptionsManagers returns the RuntimeOptionsManager.
	RuntimeOptionsManager() m3dbruntime.OptionsManager

	// SetContextPool sets the contextPool.
	SetContextPool(value context.Pool) Options

	// ContextPool returns the contextPool.
	ContextPool() context.Pool

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) Options

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetIndexOptions set the indexing options.
	SetIndexOptions(value index.Options) Options

	// IndexOptions returns the indexing options.
	IndexOptions() index.Options
}
