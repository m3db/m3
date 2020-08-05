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
	"errors"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
)

var (
	errNewMergerFnNotSet       = errors.New("newMergerFn not set")
	errInfoFilesResultError    = errors.New("infoFileResult contains an error")
	errNamespaceMetadataNotSet = errors.New("namespaceMetadata not set")
	errPersistManagerNotSet    = errors.New("persistManager not set")
	errStorageOptionsNotSet    = errors.New("storageOptions not set")
	errFilesystemOptionsNotSet = errors.New("filesystemOptions not set")
)

type taskOptions struct {
	newMergerFn       fs.NewMergerFn
	infoFileResult    fs.ReadInfoFileResult
	shard             uint32
	namespaceMetadata namespace.Metadata
	persistManager    persist.Manager
	storageOptions    storage.Options
	fsOpts            fs.Options
}

var _ TaskOptions = &taskOptions{}

// NewTaskOptions creates new taskOptions
func NewTaskOptions() TaskOptions {
	return &taskOptions{
		newMergerFn: fs.NewMerger,
	}
}

func (t *taskOptions) Validate() error {
	if t.storageOptions == nil {
		return errStorageOptionsNotSet
	}
	if err := t.storageOptions.Validate(); err != nil {
		return err
	}
	if t.newMergerFn == nil {
		return errNewMergerFnNotSet
	}
	if t.infoFileResult.Err != nil && t.infoFileResult.Err.Error() != nil {
		return errInfoFilesResultError
	}
	if t.namespaceMetadata == nil {
		return errNamespaceMetadataNotSet
	}
	if t.persistManager == nil {
		return errPersistManagerNotSet
	}
	if t.fsOpts == nil {
		return errFilesystemOptionsNotSet
	}
	if err := t.fsOpts.Validate(); err != nil {
		return err
	}

	return nil
}

func (t *taskOptions) SetNewMergerFn(value fs.NewMergerFn) TaskOptions {
	to := *t
	to.newMergerFn = value
	return &to
}

func (t *taskOptions) NewMergerFn() fs.NewMergerFn {
	return t.newMergerFn
}

func (t *taskOptions) SetInfoFileResult(value fs.ReadInfoFileResult) TaskOptions {
	to := *t
	to.infoFileResult = value
	return &to
}

func (t *taskOptions) InfoFileResult() fs.ReadInfoFileResult {
	return t.infoFileResult
}

func (t *taskOptions) SetShard(value uint32) TaskOptions {
	to := *t
	to.shard = value
	return &to
}

func (t *taskOptions) Shard() uint32 {
	return t.shard
}

func (t *taskOptions) SetNamespaceMetadata(value namespace.Metadata) TaskOptions {
	to := *t
	to.namespaceMetadata = value
	return &to
}

func (t *taskOptions) NamespaceMetadata() namespace.Metadata {
	return t.namespaceMetadata
}

func (t *taskOptions) SetPersistManager(value persist.Manager) TaskOptions {
	to := *t
	to.persistManager = value
	return &to
}

func (t *taskOptions) PersistManager() persist.Manager {
	return t.persistManager
}

func (t *taskOptions) SetStorageOptions(value storage.Options) TaskOptions {
	to := *t
	to.storageOptions = value
	return &to
}

func (t *taskOptions) StorageOptions() storage.Options {
	return t.storageOptions
}

func (t *taskOptions) SetFilesystemOptions(value fs.Options) TaskOptions {
	to := *t
	to.fsOpts = value
	return &to
}

func (t *taskOptions) FilesystemOptions() fs.Options {
	return t.fsOpts
}
