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
	"github.com/m3db/m3db/retention"
)

const (
	// Namespace requires bootstrapping by default
	defaultNeedsBootstrap = true

	// Namespace requires flushing by default
	defaultNeedsFlush = true

	// Namespace requires snapshotting by default
	defaultNeedsSnapshot = true

	// Namespace writes go to commit logs by default
	defaultWritesToCommitLog = true

	// Namespace requires fileset/snapshot cleanup by default
	defaultNeedsCleanup = true

	// Namespace requires repair by default
	defaultNeedsRepair = true
)

type options struct {
	needsBootstrap    bool
	needsFlush        bool
	needsSnapshot     bool
	writesToCommitLog bool
	needsCleanup      bool
	needsRepair       bool
	retentionOpts     retention.Options
}

// NewOptions creates a new namespace options
func NewOptions() Options {
	return &options{
		needsBootstrap:    defaultNeedsBootstrap,
		needsFlush:        defaultNeedsFlush,
		needsSnapshot:     defaultNeedsSnapshot,
		writesToCommitLog: defaultWritesToCommitLog,
		needsCleanup:      defaultNeedsCleanup,
		needsRepair:       defaultNeedsRepair,
		retentionOpts:     retention.NewOptions(),
	}
}

func (o *options) Validate() error {
	return o.retentionOpts.Validate()
}

func (o *options) Equal(value Options) bool {
	return o.needsBootstrap == value.NeedsBootstrap() &&
		o.needsFlush == value.FlushEnabled() &&
		o.writesToCommitLog == value.WritesToCommitLog() &&
		o.needsCleanup == value.CleanupEnabled() &&
		o.needsRepair == value.RepairEnabled() &&
		o.retentionOpts.Equal(value.RetentionOptions())
}

func (o *options) SetNeedsBootstrap(value bool) Options {
	opts := *o
	opts.needsBootstrap = value
	return &opts
}

func (o *options) NeedsBootstrap() bool {
	return o.needsBootstrap
}

func (o *options) SetFlushEnabled(value bool) Options {
	opts := *o
	opts.needsFlush = value
	return &opts
}

func (o *options) FlushEnabled() bool {
	return o.needsFlush
}

func (o *options) SetSnapshotEnabled(value bool) Options {
	opts := *o
	opts.needsSnapshot = value
	return &opts
}

func (o *options) SnapshotEnabled() bool {
	return o.needsSnapshot
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
	opts.needsCleanup = value
	return &opts
}

func (o *options) CleanupEnabled() bool {
	return o.needsCleanup
}

func (o *options) SetRepairEnabled(value bool) Options {
	opts := *o
	opts.needsRepair = value
	return &opts
}

func (o *options) RepairEnabled() bool {
	return o.needsRepair
}

func (o *options) SetRetentionOptions(value retention.Options) Options {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *options) RetentionOptions() retention.Options {
	return o.retentionOpts
}
