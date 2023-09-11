// Copyright (c) 2017 Uber Technologies, Inc.
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

package cache

import (
	"time"

	"github.com/m3db/m3/src/metrics/matcher/namespace"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

// InvalidationMode is the invalidation mode.
type InvalidationMode int

const (
	// InvalidateOne only invalidates a single invalid entry as needed.
	InvalidateOne InvalidationMode = iota

	// InvalidateAll invalidates all entries as long as one entry is invalid.
	InvalidateAll
)

const (
	defaultCapacity          = 200000
	defaultFreshDuration     = 5 * time.Minute
	defaultStutterDuration   = time.Minute
	defaultEvictionBatchSize = 1024
	defaultDeletionBatchSize = 1024
	defaultInvalidationMode  = InvalidateAll
)

// Options provide a set of cache options.
type Options interface {
	// SetClockOptions sets the clock options.
	SetClockOptions(value clock.Options) Options

	// ClockOptions returns the clock options.
	ClockOptions() clock.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetCapacity sets the cache capacity.
	SetCapacity(value int) Options

	// Capacity returns the cache capacity.
	Capacity() int

	// SetFreshDuration sets the entry fresh duration.
	SetFreshDuration(value time.Duration) Options

	// FreshDuration returns the fresh duration.
	FreshDuration() time.Duration

	// SetStutterDuration sets the entry stutter duration.
	SetStutterDuration(value time.Duration) Options

	// StutterDuration returns the entry stutter duration.
	StutterDuration() time.Duration

	// SetEvictionBatchSize sets the eviction batch size.
	SetEvictionBatchSize(value int) Options

	// EvictionBatchSize returns the eviction batch size.
	EvictionBatchSize() int

	// SetDeletionBatchSize sets the deletion batch size.
	SetDeletionBatchSize(value int) Options

	// DeletionBatchSize returns the deletion batch size.
	DeletionBatchSize() int

	// SetInvalidationMode sets the invalidation mode.
	SetInvalidationMode(value InvalidationMode) Options

	// InvalidationMode returns the invalidation mode.
	InvalidationMode() InvalidationMode

	// NamespaceResolver returns the namespace Resolver.
	NamespaceResolver() namespace.Resolver

	// SetNamespaceResolver set the NamespaceResolver.
	SetNamespaceResolver(value namespace.Resolver) Options
}

type options struct {
	clockOpts         clock.Options
	instrumentOpts    instrument.Options
	capacity          int
	freshDuration     time.Duration
	stutterDuration   time.Duration
	evictionBatchSize int
	deletionBatchSize int
	invalidationMode  InvalidationMode
	nsResolver        namespace.Resolver
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{
		clockOpts:         clock.NewOptions(),
		instrumentOpts:    instrument.NewOptions(),
		capacity:          defaultCapacity,
		freshDuration:     defaultFreshDuration,
		stutterDuration:   defaultStutterDuration,
		evictionBatchSize: defaultEvictionBatchSize,
		deletionBatchSize: defaultDeletionBatchSize,
		invalidationMode:  defaultInvalidationMode,
		nsResolver:        namespace.Default,
	}
}

func (o *options) SetClockOptions(value clock.Options) Options {
	opts := *o
	opts.clockOpts = value
	return &opts
}

func (o *options) ClockOptions() clock.Options {
	return o.clockOpts
}

func (o *options) SetInstrumentOptions(value instrument.Options) Options {
	opts := *o
	opts.instrumentOpts = value
	return &opts
}

func (o *options) InstrumentOptions() instrument.Options {
	return o.instrumentOpts
}

func (o *options) SetCapacity(value int) Options {
	opts := *o
	opts.capacity = value
	return &opts
}

func (o *options) Capacity() int {
	return o.capacity
}

func (o *options) SetFreshDuration(value time.Duration) Options {
	opts := *o
	opts.freshDuration = value
	return &opts
}

func (o *options) FreshDuration() time.Duration {
	return o.freshDuration
}

func (o *options) SetStutterDuration(value time.Duration) Options {
	opts := *o
	opts.stutterDuration = value
	return &opts
}

func (o *options) StutterDuration() time.Duration {
	return o.stutterDuration
}

func (o *options) SetEvictionBatchSize(value int) Options {
	opts := *o
	opts.evictionBatchSize = value
	return &opts
}

func (o *options) EvictionBatchSize() int {
	return o.evictionBatchSize
}

func (o *options) SetDeletionBatchSize(value int) Options {
	opts := *o
	opts.deletionBatchSize = value
	return &opts
}

func (o *options) DeletionBatchSize() int {
	return o.deletionBatchSize
}

func (o *options) SetInvalidationMode(value InvalidationMode) Options {
	opts := *o
	opts.invalidationMode = value
	return &opts
}

func (o *options) InvalidationMode() InvalidationMode {
	return o.invalidationMode
}

func (o *options) NamespaceResolver() namespace.Resolver {
	return o.nsResolver
}

func (o *options) SetNamespaceResolver(value namespace.Resolver) Options {
	opts := *o
	opts.nsResolver = value
	return &opts
}
