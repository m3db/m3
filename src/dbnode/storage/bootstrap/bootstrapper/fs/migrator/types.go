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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/instrument"
)

// NewMigrationFn is a function that creates a new migration task
type NewMigrationFn func(opts migration.TaskOptions) (migration.Task, error)

// ShouldMigrateFn is a function that determines if a fileset should be migrated
type ShouldMigrateFn func(result fs.ReadInfoFileResult) bool

// Options represents the options for the migrator
type Options interface {
	// Validate checks that options are valid
	Validate() error

	// SetNewMigrationFn sets the function for creating a new migration
	SetNewMigrationFn(value NewMigrationFn) Options

	// NewMigrationFn gets the function for creating a new migration
	NewMigrationFn() NewMigrationFn

	// SetShouldMigrateFn sets the function for determining if the migrator should migrate a fileset
	SetShouldMigrateFn(value ShouldMigrateFn) Options

	// ShouldMigrateFn gets the function for determining if the migrator should migrate a fileset
	ShouldMigrateFn() ShouldMigrateFn

	// SetInfoFilesByNamespaces sets the info file results to operate on keyed by namespace
	SetInfoFilesByNamespace(value map[namespace.Metadata]fs.ShardsInfoFilesResult) Options

	// InfoFilesByNamespaces returns the info file results to operate on keyed by namespace
	InfoFilesByNamespace() map[namespace.Metadata]fs.ShardsInfoFilesResult

	// SetMigrationOptions sets the migration options
	SetMigrationOptions(value migration.Options) Options

	// MigrationOptions returns the migration options
	MigrationOptions() migration.Options

	// SetFilesystemOptions sets the filesystem options
	SetFilesystemOptions(value fs.Options) Options

	// FileSystemOptions returns the filesystem options
	FilesystemOptions() fs.Options

	// SetInstrumentOptions sets the instrument options
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options
	InstrumentOptions() instrument.Options

	// SetStorageOptions sets the storage options
	SetStorageOptions(value storage.Options) Options

	// StorageOptions returns the storage options
	StorageOptions() storage.Options
}
