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

package namespace

import (
	"errors"

	"github.com/m3db/m3db/retention"
)

const (
	// Namespace requires bootstrapping by default
	defaultBootstrapEnabled = true

	// Namespace requires flushing by default
	defaultFlushEnabled = true

	// Namespace requires snapshotting by default
	defaultSnapshotEnabled = true

	// Namespace writes go to commit logs by default
	defaultWritesToCommitLog = true

	// Namespace requires fileset/snapshot cleanup by default
	defaultCleanupEnabled = true

	// Namespace requires repair by default
	defaultRepairEnabled = true
)

var (
	errIndexBlockSizeTooLarge                     = errors.New("index block size needs to be <= namespace retention period")
	errIndexBlockSizeMustBeBiggerThanBufferFuture = errors.New("index block size must be >= namespace buffer future")
	errIndexBlockSizeMustBeBiggerThanBufferPast   = errors.New("index block size must be >= namespace buffer past")
)

type options struct {
	bootstrapEnabled  bool
	flushEnabled      bool
	snapshotEnabled   bool
	writesToCommitLog bool
	cleanupEnabled    bool
	repairEnabled     bool
	retentionOpts     retention.Options
	indexOpts         IndexOptions
}

// NewOptions creates a new namespace options
func NewOptions() Options {
	return &options{
		bootstrapEnabled:  defaultBootstrapEnabled,
		flushEnabled:      defaultFlushEnabled,
		snapshotEnabled:   defaultSnapshotEnabled,
		writesToCommitLog: defaultWritesToCommitLog,
		cleanupEnabled:    defaultCleanupEnabled,
		repairEnabled:     defaultRepairEnabled,
		retentionOpts:     retention.NewOptions(),
		indexOpts:         NewIndexOptions(),
	}
}

func (o *options) Validate() error {
	if err := o.retentionOpts.Validate(); err != nil {
		return err
	}
	if !o.indexOpts.Enabled() {
		return nil
	}
	if err := o.indexOpts.Validate(); err != nil {
		return err
	}
	var (
		retention      = o.retentionOpts.RetentionPeriod()
		indexBlockSize = o.indexOpts.BlockSize()
	)
	if retention < indexBlockSize {
		return errIndexBlockSizeTooLarge
	}
	if indexBlockSize < o.retentionOpts.BufferFuture() {
		return errIndexBlockSizeMustBeBiggerThanBufferFuture
	}
	if indexBlockSize < o.retentionOpts.BufferPast() {
		return errIndexBlockSizeMustBeBiggerThanBufferPast
	}
	return nil
}

func (o *options) Equal(value Options) bool {
	return o.bootstrapEnabled == value.BootstrapEnabled() &&
		o.flushEnabled == value.FlushEnabled() &&
		o.writesToCommitLog == value.WritesToCommitLog() &&
		o.snapshotEnabled == value.SnapshotEnabled() &&
		o.cleanupEnabled == value.CleanupEnabled() &&
		o.repairEnabled == value.RepairEnabled() &&
		o.retentionOpts.Equal(value.RetentionOptions()) &&
		o.indexOpts.Equal(value.IndexOptions())
}

func (o *options) SetBootstrapEnabled(value bool) Options {
	opts := *o
	opts.bootstrapEnabled = value
	return &opts
}

func (o *options) BootstrapEnabled() bool {
	return o.bootstrapEnabled
}

func (o *options) SetFlushEnabled(value bool) Options {
	opts := *o
	opts.flushEnabled = value
	return &opts
}

func (o *options) FlushEnabled() bool {
	return o.flushEnabled
}

func (o *options) SetSnapshotEnabled(value bool) Options {
	opts := *o
	opts.snapshotEnabled = value
	return &opts
}

func (o *options) SnapshotEnabled() bool {
	return o.snapshotEnabled
}

func (o *options) SetWritesToCommitLog(value bool) Options {
	opts := *o
	opts.writesToCommitLog = value
	return &opts
}

func (o *options) WritesToCommitLog() bool {
	return o.writesToCommitLog
}

func (o *options) SetCleanupEnabled(value bool) Options {
	opts := *o
	opts.cleanupEnabled = value
	return &opts
}

func (o *options) CleanupEnabled() bool {
	return o.cleanupEnabled
}

func (o *options) SetRepairEnabled(value bool) Options {
	opts := *o
	opts.repairEnabled = value
	return &opts
}

func (o *options) RepairEnabled() bool {
	return o.repairEnabled
}

func (o *options) SetRetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) RetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *options) SetIndexOptions(value IndexOptions) Options {
	opts := *o
	opts.indexOpts = value
	return &opts
}

func (o *options) IndexOptions() IndexOptions {
	return o.indexOpts
}
