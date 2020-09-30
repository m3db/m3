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
	"errors"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errMigrationTaskFnNotSet      = errors.New("migrationTaskFn not set")
	errInfoFilesByNamespaceNotSet = errors.New("infoFilesByNamespaces not set")
	errMigrationOptsNotSet        = errors.New("migrationOpts not set")
	errInstrumentOptsNotSet       = errors.New("instrumentOpts not set")
	errStorageOptsNotSet          = errors.New("storageOpts not set")
	errFilesystemOptsNotSet       = errors.New("filesystemOpts not set")
)

type options struct {
	migrationTaskFn      MigrationTaskFn
	infoFilesByNamespace bootstrap.InfoFilesByNamespace
	migrationOpts        migration.Options
	fsOpts               fs.Options
	instrumentOpts       instrument.Options
	storageOpts          storage.Options
}

// NewOptions return new migration opts
func NewOptions() Options {
	return &options{
		migrationOpts:  migration.NewOptions(),
		instrumentOpts: instrument.NewOptions(),
	}
}

func (o *options) Validate() error {
	if o.storageOpts == nil {
		return errStorageOptsNotSet
	}
	if err := o.storageOpts.Validate(); err != nil {
		return err
	}
	if o.migrationTaskFn == nil {
		return errMigrationTaskFnNotSet
	}
	if o.infoFilesByNamespace == nil {
		return errInfoFilesByNamespaceNotSet
	}
	if o.migrationOpts == nil {
		return errMigrationOptsNotSet
	}
	if o.instrumentOpts == nil {
		return errInstrumentOptsNotSet
	}
	if o.fsOpts == nil {
		return errFilesystemOptsNotSet
	}
	if err := o.fsOpts.Validate(); err != nil {
		return err
	}
	return nil
}

func (o *options) SetMigrationTaskFn(value MigrationTaskFn) Options {
	opts := *o
	opts.migrationTaskFn = value
	return &opts
}

func (o *options) MigrationTaskFn() MigrationTaskFn {
	return o.migrationTaskFn
}

func (o *options) SetInfoFilesByNamespace(value bootstrap.InfoFilesByNamespace) Options {
	opts := *o
	opts.infoFilesByNamespace = value
	return &opts
}

func (o *options) InfoFilesByNamespace() bootstrap.InfoFilesByNamespace {
	return o.infoFilesByNamespace
}

func (o *options) SetMigrationOptions(value migration.Options) Options {
	opts := *o
	opts.migrationOpts = value
	return &opts
}

func (o *options) MigrationOptions() migration.Options {
	return o.migrationOpts
}

func (o *options) SetFilesystemOptions(value fs.Options) Options {
	opts := *o
	opts.fsOpts = value
	return &opts
}

func (o *options) FilesystemOptions() fs.Options {
	return o.fsOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetStorageOptions(value storage.Options) Options {
	opts := *o
	opts.storageOpts = value
	return &opts
}

func (o *options) StorageOptions() storage.Options {
	return o.storageOpts
}
