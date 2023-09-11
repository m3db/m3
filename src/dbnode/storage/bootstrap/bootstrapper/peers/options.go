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
	"errors"
	"fmt"
	"math"
	"runtime"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	m3dbruntime "github.com/m3db/m3/src/dbnode/runtime"
	fsbootstrapper "github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/pool"
)

var (
	// DefaultShardConcurrency controls how many shards in parallel to stream
	// for in memory data being streamed between peers (most recent block).
	// Update BootstrapPeersConfiguration comment in
	// src/cmd/services/m3dbnode/config package if this is changed.
	DefaultShardConcurrency = runtime.GOMAXPROCS(0)
	// DefaultShardPersistenceConcurrency controls how many shards in parallel to stream
	// for historical data being streamed between peers (historical blocks).
	// Update BootstrapPeersConfiguration comment in
	// src/cmd/services/m3dbnode/config package if this is changed.
	DefaultShardPersistenceConcurrency = int(math.Max(1, float64(runtime.GOMAXPROCS(0))/2))
	defaultPersistenceMaxQueueSize     = 0
	// DefaultShardPersistenceFlushConcurrency controls how many shards in parallel to flush
	// for historical data being streamed between peers (historical blocks).
	// Update BootstrapPeersConfiguration comment in
	// src/cmd/services/m3dbnode/config package if this is changed.
	DefaultShardPersistenceFlushConcurrency = 1
)

var (
	errAdminClientNotSet           = errors.New("admin client not set")
	errPersistManagerNotSet        = errors.New("persist manager not set")
	errIndexClaimsManagerNotSet    = errors.New("index claims manager not set")
	errCompactorNotSet             = errors.New("compactor not set")
	errIndexOptionsNotSet          = errors.New("index options not set")
	errFilesystemOptionsNotSet     = errors.New("filesystem options not set")
	errRuntimeOptionsManagerNotSet = errors.New("runtime options manager not set")
)

type options struct {
	resultOpts                       result.Options
	client                           client.AdminClient
	defaultShardConcurrency          int
	shardPersistenceConcurrency      int
	shardPersistenceFlushConcurrency int
	indexSegmentConcurrency          int
	persistenceMaxQueueSize          int
	persistManager                   persist.Manager
	indexClaimsManager               fs.IndexClaimsManager
	runtimeOptionsManager            m3dbruntime.OptionsManager
	contextPool                      context.Pool
	fsOpts                           fs.Options
	indexOpts                        index.Options
	compactor                        *compaction.Compactor
}

// NewOptions creates new bootstrap options.
func NewOptions() Options {
	return &options{
		resultOpts:                       result.NewOptions(),
		defaultShardConcurrency:          DefaultShardConcurrency,
		shardPersistenceConcurrency:      DefaultShardPersistenceConcurrency,
		shardPersistenceFlushConcurrency: DefaultShardPersistenceFlushConcurrency,
		indexSegmentConcurrency:          fsbootstrapper.DefaultIndexSegmentConcurrency,
		persistenceMaxQueueSize:          defaultPersistenceMaxQueueSize,
		// Use a zero pool, this should be overridden at config time.
		contextPool: context.NewPool(context.NewOptions().
			SetContextPoolOptions(pool.NewObjectPoolOptions().SetSize(0)).
			SetFinalizerPoolOptions(pool.NewObjectPoolOptions().SetSize(0))),
	}
}

func (o *options) Validate() error {
	if client := o.client; client == nil {
		return errAdminClientNotSet
	}
	if o.persistManager == nil {
		return errPersistManagerNotSet
	}
	if o.indexClaimsManager == nil {
		return errIndexClaimsManagerNotSet
	}
	if o.compactor == nil {
		return errCompactorNotSet
	}
	if o.runtimeOptionsManager == nil {
		return errRuntimeOptionsManagerNotSet
	}
	if o.indexOpts == nil {
		return errIndexOptionsNotSet
	}
	if o.fsOpts == nil {
		return errFilesystemOptionsNotSet
	}
	if n := o.indexSegmentConcurrency; n <= 0 {
		return fmt.Errorf("index segment concurrency not >= 1: actual=%d", n)
	}
	if n := o.shardPersistenceConcurrency; n <= 0 {
		return fmt.Errorf("shard persistence concurrency not >= 1: actual=%d", n)
	}
	if n := o.shardPersistenceFlushConcurrency; n <= 0 {
		return fmt.Errorf("shard persistence flush concurrency not >= 1: actual=%d", n)
	}
	if n := o.defaultShardConcurrency; n <= 0 {
		return fmt.Errorf("default shard concurrency not >= 1: actual=%d", n)
	}
	return nil
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.client = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.client
}

func (o *options) SetDefaultShardConcurrency(value int) Options {
	opts := *o
	opts.defaultShardConcurrency = value
	return &opts
}

func (o *options) DefaultShardConcurrency() int {
	return o.defaultShardConcurrency
}

func (o *options) SetShardPersistenceConcurrency(value int) Options {
	opts := *o
	opts.shardPersistenceConcurrency = value
	return &opts
}

func (o *options) ShardPersistenceConcurrency() int {
	return o.shardPersistenceConcurrency
}

func (o *options) SetShardPersistenceFlushConcurrency(value int) Options {
	opts := *o
	opts.shardPersistenceFlushConcurrency = value
	return &opts
}

func (o *options) ShardPersistenceFlushConcurrency() int {
	return o.shardPersistenceFlushConcurrency
}

func (o *options) SetIndexSegmentConcurrency(value int) Options {
	opts := *o
	opts.indexSegmentConcurrency = value
	return &opts
}

func (o *options) IndexSegmentConcurrency() int {
	return o.indexSegmentConcurrency
}

func (o *options) SetPersistenceMaxQueueSize(value int) Options {
	opts := *o
	opts.persistenceMaxQueueSize = value
	return &opts
}

func (o *options) PersistenceMaxQueueSize() int {
	return o.persistenceMaxQueueSize
}

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetIndexClaimsManager(value fs.IndexClaimsManager) Options {
	opts := *o
	opts.indexClaimsManager = value
	return &opts
}

func (o *options) IndexClaimsManager() fs.IndexClaimsManager {
	return o.indexClaimsManager
}

func (o *options) SetCompactor(value *compaction.Compactor) Options {
	opts := *o
	opts.compactor = value
	return &opts
}

func (o *options) Compactor() *compaction.Compactor {
	return o.compactor
}

func (o *options) SetRuntimeOptionsManager(value m3dbruntime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptionsManager = value
	return &opts
}

func (o *options) RuntimeOptionsManager() m3dbruntime.OptionsManager {
	return o.runtimeOptionsManager
}

func (o *options) SetContextPool(value context.Pool) Options {
	opts := *o
	opts.contextPool = value
	return &opts
}

func (o *options) ContextPool() context.Pool {
	return o.contextPool
}

func (o *options) SetFilesystemOptions(value fs.Options) Options {
	opts := *o
	opts.fsOpts = value
	return &opts
}

func (o *options) FilesystemOptions() fs.Options {
	return o.fsOpts
}

func (o *options) SetIndexOptions(value index.Options) Options {
	opts := *o
	opts.indexOpts = value
	return &opts
}

func (o *options) IndexOptions() index.Options {
	return o.indexOpts
}
