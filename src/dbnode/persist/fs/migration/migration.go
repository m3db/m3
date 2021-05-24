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
	xtime "github.com/m3db/m3/src/x/time"
)

// Task interface is implemented by tasks that wish to perform a data migration
// on a fileset. This typically involves updating files in a fileset that were created by
// a previous version of the database client.
type Task interface {
	// Run is the set of steps to successfully complete a migration. Returns the potentially
	// updated ReadInfoFileResult or an error.
	Run() (fs.ReadInfoFileResult, error)
}

// NewTaskFn is a function that can create a new migration task.
type NewTaskFn func(opts TaskOptions) (Task, error)

// toVersion1_1Task is an object responsible for migrating a fileset to version 1.1.
type toVersion1_1Task struct {
	opts TaskOptions
}

// MigrationTask returns true or false if a fileset should be migrated. If true, also returns
// a function that can be used to create a new migration task.
func MigrationTask(info fs.ReadInfoFileResult) (NewTaskFn, bool) {
	if info.Info.MajorVersion == 1 && info.Info.MinorVersion == 0 {
		return NewToVersion1_1Task, true
	}
	return nil, false
}

// NewToVersion1_1Task creates a task for migrating a fileset to version 1.1.
func NewToVersion1_1Task(opts TaskOptions) (Task, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &toVersion1_1Task{
		opts: opts,
	}, nil
}

// Run executes the steps to bring a fileset to Version 1.1.
func (v *toVersion1_1Task) Run() (fs.ReadInfoFileResult, error) {
	var (
		sOpts          = v.opts.StorageOptions()
		fsOpts         = v.opts.FilesystemOptions()
		newMergerFn    = v.opts.NewMergerFn()
		nsMd           = v.opts.NamespaceMetadata()
		infoFileResult = v.opts.InfoFileResult()
		shard          = v.opts.Shard()
		persistManager = v.opts.PersistManager()
	)
	reader, err := fs.NewReader(sOpts.BytesPool(), fsOpts)
	if err != nil {
		return infoFileResult, err
	}

	merger := newMergerFn(reader, sOpts.DatabaseBlockOptions().DatabaseBlockAllocSize(),
		sOpts.SegmentReaderPool(), sOpts.MultiReaderIteratorPool(),
		sOpts.IdentifierPool(), sOpts.EncoderPool(), sOpts.ContextPool(),
		fsOpts.FilePathPrefix(), nsMd.Options())

	volIndex := infoFileResult.Info.VolumeIndex
	fsID := fs.FileSetFileIdentifier{
		Namespace:   nsMd.ID(),
		Shard:       shard,
		BlockStart:  xtime.UnixNano(infoFileResult.Info.BlockStart),
		VolumeIndex: volIndex,
	}

	nsCtx := namespace.NewContextFrom(nsMd)

	flushPersist, err := persistManager.StartFlushPersist()
	if err != nil {
		return infoFileResult, err
	}

	// Intentionally use a noop merger here as we simply want to rewrite the same files with the current encoder which
	// will generate index files with the entry level checksums.
	newIndex := volIndex + 1
	if err = merger.MergeAndCleanup(fsID, fs.NewNoopMergeWith(), newIndex, flushPersist, nsCtx,
		&persist.NoOpColdFlushNamespace{}, false); err != nil {
		return infoFileResult, err
	}

	if err = flushPersist.DoneFlush(); err != nil {
		return infoFileResult, err
	}

	infoFileResult.Info.VolumeIndex = newIndex
	infoFileResult.Info.MinorVersion = 1

	return infoFileResult, nil
}
