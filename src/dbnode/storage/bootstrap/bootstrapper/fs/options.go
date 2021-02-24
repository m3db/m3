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
	"fmt"
	"math"
	goruntime "runtime"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
)

var (
	errPersistManagerNotSet     = errors.New("persist manager not set")
	errIndexClaimsManagerNotSet = errors.New("index claims manager not set")
	errCompactorNotSet          = errors.New("compactor not set")
	errIndexOptionsNotSet       = errors.New("index options not set")
	errFilesystemOptionsNotSet  = errors.New("filesystem options not set")
	errMigrationOptionsNotSet   = errors.New("migration options not set")

	// DefaultIndexSegmentConcurrency defines the default index segment building concurrency.
	DefaultIndexSegmentConcurrency = int(math.Min(2, float64(goruntime.NumCPU())))

	// defaultIndexSegmentsVerify defines default for index segments validation.
	defaultIndexSegmentsVerify = false
)

type options struct {
	instrumentOpts          instrument.Options
	resultOpts              result.Options
	fsOpts                  fs.Options
	indexOpts               index.Options
	persistManager          persist.Manager
	indexClaimsManager      fs.IndexClaimsManager
	compactor               *compaction.Compactor
	indexSegmentConcurrency int
	indexSegmentsVerify     bool
	runtimeOptsMgr          runtime.OptionsManager
	identifierPool          ident.Pool
	migrationOpts           migration.Options
	storageOpts             storage.Options
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	bytesPool := pool.NewCheckedBytesPool(nil, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	idPool := ident.NewPool(bytesPool, ident.PoolOptions{})

	return &options{
		instrumentOpts:          instrument.NewOptions(),
		resultOpts:              result.NewOptions(),
		indexSegmentConcurrency: DefaultIndexSegmentConcurrency,
		indexSegmentsVerify:     defaultIndexSegmentsVerify,
		runtimeOptsMgr:          runtime.NewOptionsManager(),
		identifierPool:          idPool,
		migrationOpts:           migration.NewOptions(),
		storageOpts:             storage.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.persistManager == nil {
		return errPersistManagerNotSet
	}
	if o.indexClaimsManager == nil {
		return errIndexClaimsManagerNotSet
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
	if o.migrationOpts == nil {
		return errMigrationOptionsNotSet
	}
	if err := o.migrationOpts.Validate(); err != nil {
		return err
	}
	if n := o.indexSegmentConcurrency; n <= 0 {
		return fmt.Errorf("index segment concurrency not >= 1: actual=%d", n)
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

func (o *options) SetIndexSegmentConcurrency(value int) Options {
	opts := *o
	opts.indexSegmentConcurrency = value
	return &opts
}

func (o *options) IndexSegmentConcurrency() int {
	return o.indexSegmentConcurrency
}

func (o *options) SetIndexSegmentsVerify(value bool) Options {
	opts := *o
	opts.indexSegmentsVerify = value
	return &opts
}

func (o *options) IndexSegmentsVerify() bool {
	return o.indexSegmentsVerify
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
	opts.migrationOpts = value
	return &opts
}

func (o *options) MigrationOptions() migration.Options {
	return o.migrationOpts
}

func (o *options) SetStorageOptions(value storage.Options) Options {
	opts := *o
	opts.storageOpts = value
	return &opts
}

func (o *options) StorageOptions() storage.Options {
	return o.storageOpts
}
