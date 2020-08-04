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

package migration

import (
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
)

// Options represents the options for migrations
type Options interface {
	// SetToVersion1_1 sets the toVersion1_1 migration option
	SetToVersion1_1(value bool) Options

	// ToVersion1_1 returns the value of toVersion1_1 migration option
	ToVersion1_1() bool
}

// TaskOptions represents options for individual migration tasks
type TaskOptions interface {
	// Validate validates the options
	Validate() error

	// SetNewMergerFn sets the function to create a new Merger
	SetNewMergerFn(value fs.NewMergerFn) TaskOptions

	// NewMergerFn returns the function to create a new Merger
	NewMergerFn() fs.NewMergerFn

	// SetInfoFileResult sets the info file resulted associated with this run
	SetInfoFileResult(value fs.ReadInfoFileResult) TaskOptions

	// InfoFileResult gets the info file resulted associated with this run
	InfoFileResult() fs.ReadInfoFileResult

	// SetShard sets the shard associated with this task
	SetShard(value uint32) TaskOptions

	// Shard gets the shard associated with this task
	Shard() uint32

	// SetNamespaceMetadata sets the namespace metadata associated with this task
	SetNamespaceMetadata(value namespace.Metadata) TaskOptions

	// NamespaceMetadata gets the namespace metadata associated with this task
	NamespaceMetadata() namespace.Metadata

	// SetPersistManager sets the persist manager used for this task
	SetPersistManager(value persist.Manager) TaskOptions

	// PersistManager gets the persist manager use for this task
	PersistManager() persist.Manager

	// SetStorageOptions sets the storage options associated with this task
	SetStorageOptions(value storage.Options) TaskOptions

	// StorageOptions gets the storage options associated with this task
	StorageOptions() storage.Options

	// SetFilesystemOptions sets the filesystem options.
	SetFilesystemOptions(value fs.Options) TaskOptions

	// FilesystemOptions returns the filesystem options.
	FilesystemOptions() fs.Options
}
