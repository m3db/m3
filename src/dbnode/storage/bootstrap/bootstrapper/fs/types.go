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
)

// Options represents the options for bootstrapping from the filesystem.
type Options interface {
	// Validate validates the options are correct
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetResultOptions sets the instrumentation options.
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the instrumentation options.
	ResultOptions() result.Options

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) Options

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetPersistManager sets the persistence manager used to flush blocks
	// when performing a bootstrap run with persistence enabled.
	SetPersistManager(value persist.Manager) Options

	// PersistManager returns the persistence manager used to flush blocks
	// when performing a bootstrap run with persistence enabled.
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

	// SetIndexSegmentConcurrency sets the concurrency for
	// building index segments.
	SetIndexSegmentConcurrency(value int) Options

	// IndexSegmentConcurrency returns the concurrency for
	// building index segments.
	IndexSegmentConcurrency() int

	// SetIndexSegmentsVerify sets the value for whether to verify bootstrapped
	// index segments.
	SetIndexSegmentsVerify(value bool) Options

	// IndexSegmentsVerify returns the value for whether to verify bootstrapped
	// index segments.
	IndexSegmentsVerify() bool

	// SetRuntimeOptionsManager sets the runtime options manager.
	SetRuntimeOptionsManager(value runtime.OptionsManager) Options

	// RuntimeOptionsManager returns the runtime options manager.
	RuntimeOptionsManager() runtime.OptionsManager

	// SetIdentifierPool sets the identifier pool.
	SetIdentifierPool(value ident.Pool) Options

	// IdentifierPool returns the identifier pool.
	IdentifierPool() ident.Pool

	// SetIndexOptions set the indexing options.
	SetIndexOptions(value index.Options) Options

	// IndexOptions returns the indexing options.
	IndexOptions() index.Options

	// SetMigrationOptions sets the migration options.
	SetMigrationOptions(value migration.Options) Options

	// MigrationOptions gets the migration options.
	MigrationOptions() migration.Options

	// SetStorageOptions sets storage options.
	SetStorageOptions(value storage.Options) Options

	// StorageOptions gets the storage options.
	StorageOptions() storage.Options
}
