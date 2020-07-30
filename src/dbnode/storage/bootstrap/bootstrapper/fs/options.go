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

package fs

import (
	"errors"
	"math"
	goruntime "runtime"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

var (
	errPersistManagerNotSet    = errors.New("persist manager not set")
	errCompactorNotSet         = errors.New("compactor not set")
	errIndexOptionsNotSet      = errors.New("index options not set")
	errFilesystemOptionsNotSet = errors.New("filesystem options not set")

	// NB(r): Bootstrapping data doesn't use large amounts of memory
	// that won't be released, so its fine to do this as fast as possible.
	defaultBootstrapDataNumProcessors = int(math.Ceil(float64(goruntime.NumCPU()) / 2))
	// NB(r): Bootstrapping index segments pulls a lot of data into memory
	// since its across all shards, so we actually break up the
	// number of segments we even create across the set of shards if
	// we have to create an FST in place, this is to avoid OOMing a node.
	// Because of this we only want to create one segment at a time otherwise
	// us splitting an index block into smaller pieces is moot because we'll
	// pull a lot more data into memory if we create more than one at a time.
	defaultBootstrapIndexNumProcessors = 1
)

type options struct {
	instrumentOpts              instrument.Options
	resultOpts                  result.Options
	fsOpts                      fs.Options
	indexOpts                   index.Options
	persistManager              persist.Manager
	compactor                   *compaction.Compactor
	bootstrapDataNumProcessors  int
	bootstrapIndexNumProcessors int
	runtimeOptsMgr              runtime.OptionsManager
	identifierPool              ident.Pool
	migrationOptions            migration.Options
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})

	return &options{
		instrumentOpts:              instrument.NewOptions(),
		resultOpts:                  result.NewOptions(),
		bootstrapDataNumProcessors:  defaultBootstrapDataNumProcessors,
		bootstrapIndexNumProcessors: defaultBootstrapIndexNumProcessors,
		runtimeOptsMgr:              runtime.NewOptionsManager(),
		identifierPool:              idPool,
		migrationOptions:            migration.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.persistManager == nil {
		return errPersistManagerNotSet
	}
	if o.compactor == nil {
		return errCompactorNotSet
	}
	if o.indexOpts == nil {
		return errIndexOptionsNotSet
	}
	if o.fsOpts == nil {
		return errFilesystemOptionsNotSet
	}
	return nil
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
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

func (o *options) SetPersistManager(value persist.Manager) Options {
	opts := *o
	opts.persistManager = value
	return &opts
}

func (o *options) PersistManager() persist.Manager {
	return o.persistManager
}

func (o *options) SetCompactor(value *compaction.Compactor) Options {
	opts := *o
	opts.compactor = value
	return &opts
}

func (o *options) Compactor() *compaction.Compactor {
	return o.compactor
}

func (o *options) SetBoostrapDataNumProcessors(value int) Options {
	opts := *o
	opts.bootstrapDataNumProcessors = value
	return &opts
}

func (o *options) BoostrapDataNumProcessors() int {
	return o.bootstrapDataNumProcessors
}

func (o *options) SetBoostrapIndexNumProcessors(value int) Options {
	opts := *o
	opts.bootstrapIndexNumProcessors = value
	return &opts
}

func (o *options) BoostrapIndexNumProcessors() int {
	return o.bootstrapIndexNumProcessors
}

func (o *options) SetRuntimeOptionsManager(value runtime.OptionsManager) Options {
	opts := *o
	opts.runtimeOptsMgr = value
	return &opts
}

func (o *options) RuntimeOptionsManager() runtime.OptionsManager {
	return o.runtimeOptsMgr
}

func (o *options) SetIdentifierPool(value ident.Pool) Options {
	opts := *o
	opts.identifierPool = value
	return &opts
}

func (o *options) IdentifierPool() ident.Pool {
	return o.identifierPool
}

func (o *options) SetMigrationOptions(value migration.Options) Options {
	opts := *o
	opts.migrationOptions = value
	return &opts
}

func (o *options) MigrationOptions() migration.Options {
	return o.migrationOptions
}
