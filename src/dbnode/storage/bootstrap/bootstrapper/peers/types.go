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
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
)

// Options represents the options for bootstrapping from peers
type Options interface {
	// Validate validates the options
	Validate() error

	// SetResultOptions sets the instrumentation options
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the instrumentation options
	ResultOptions() result.Options

	// SetAdminClient sets the admin client
	SetAdminClient(value client.AdminClient) Options

	// AdminClient returns the admin client
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

	// SetDatabaseBlockRetrieverManager sets the block retriever manager to
	// pass to newly flushed blocks when performing a bootstrap run with
	// persistence enabled.
	SetDatabaseBlockRetrieverManager(
		value block.DatabaseBlockRetrieverManager,
	) Options

	// NewBlockRetrieverFn returns the block retriever manager to
	// pass to newly flushed blocks when performing a bootstrap run with
	// persistence enabled.
	DatabaseBlockRetrieverManager() block.DatabaseBlockRetrieverManager

	// SetFetchBlocksMetadataEndpointVersion sets the version of the fetch blocks
	// metadata endpoint that the peer bootstrapper will use
	SetFetchBlocksMetadataEndpointVersion(value client.FetchBlocksMetadataEndpointVersion) Options

	// SetFetchBlocksMetadataEndpointVersion returns the version of the fetch blocks
	// metadata endpoint that the peer bootstrapper will use
	FetchBlocksMetadataEndpointVersion() client.FetchBlocksMetadataEndpointVersion

	// SetRuntimeOptionsManagers sets the RuntimeOptionsManager.
	SetRuntimeOptionsManager(value m3dbruntime.OptionsManager) Options

	// RuntimeOptionsManagers returns the RuntimeOptionsManager.
	RuntimeOptionsManager() m3dbruntime.OptionsManager
}
