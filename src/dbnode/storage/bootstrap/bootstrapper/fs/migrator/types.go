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

package migrator

import (
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/instrument"
)

// MigrationTaskFn returns a fileset migration function and a boolean indicating if migration is necessary.
type MigrationTaskFn func(result fs.ReadInfoFileResult) (migration.NewTaskFn, bool)

// Options represents the options for the migrator.
type Options interface {
	// Validate checks that options are valid.
	Validate() error

	// SetMigrationTaskFn sets the function for determining if the migrator should migrate a fileset.
	SetMigrationTaskFn(value MigrationTaskFn) Options

	// MigrationTaskFn gets the function for determining if the migrator should migrate a fileset.
	MigrationTaskFn() MigrationTaskFn

	// SetInfoFilesByNamespaces sets the info file results to operate on keyed by namespace.
	SetInfoFilesByNamespace(value bootstrap.InfoFilesByNamespace) Options

	// InfoFilesByNamespaces returns the info file results to operate on keyed by namespace.
	InfoFilesByNamespace() bootstrap.InfoFilesByNamespace

	// SetMigrationOptions sets the migration options.
	SetMigrationOptions(value migration.Options) Options

	// MigrationOptions returns the migration options.
	MigrationOptions() migration.Options

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) Options

	// FileSystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetStorageOptions sets the storage options.
	SetStorageOptions(value storage.Options) Options

	// StorageOptions returns the storage options.
	StorageOptions() storage.Options
}
