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

package retention

import (
	"errors"
	"time"
)

const (
	// defaultRetentionPeriod is how long we keep data in memory by default.
	defaultRetentionPeriod = 2 * 24 * time.Hour

	// defaultBlockSize is the default block size
	defaultBlockSize = 2 * time.Hour

	// defaultBufferFuture is the default buffer future limit
	defaultBufferFuture = 2 * time.Minute

	// defaultBufferPast is the default buffer past limit
	defaultBufferPast = 10 * time.Minute

	// defaultDataExpiry is the default bool for whether data expiry is on
	defaultDataExpiry = true

	// defaultDataExpiryAfterNotAccessedPeriod is the default data expiry after not accessed period
	defaultDataExpiryAfterNotAccessedPeriod = 5 * time.Minute

	// defaultNonRealtimeWritesEnabled is the default setting on whether to accept writes to any time
	defaultNonRealtimeWritesEnabled = false

	// defaultNonRealtimeFlushAfterNoMetricPeriod is the period of no metrics after which a non-realtime metric
	// block should be flushed
	defaultNonRealtimeFlushAfterNoMetricPeriod = 5 * time.Minute

	// defaultNonRealtimeMaxWritesBeforeFlush is the max number writes in non-realtime blocks after which
	// block should be flushed
	defaultNonRealtimeMaxWritesBeforeFlush = 1<<16 - 1
)

var (
	errBufferFutureNonNegative = errors.New("buffer future must be non-negative")
	errBufferPastNonNegative   = errors.New("buffer past must be non-negative")
	errBlockSizePositive       = errors.New("block size must positive")
	errBufferFutureTooLarge    = errors.New("buffer future must be smaller than block size")
	errBufferPastTooLarge      = errors.New("buffer past must be smaller than block size")
	errRetentionPeriodTooSmall = errors.New("retention period must not be smaller than block size")
)

type options struct {
	retentionPeriod                     time.Duration
	blockSize                           time.Duration
	bufferFuture                        time.Duration
	bufferPast                          time.Duration
	dataExpiryAfterNotAccessedPeriod    time.Duration
	nonRealtimeFlushAfterNoMetricPeriod time.Duration
	nonRealtimeMaxWritesBeforeFlush     uint64
	dataExpiry                          bool
	nonRealtimeWritesEnabled            bool
}

// NewOptions creates new retention options
func NewOptions() Options {
	return &options{
		retentionPeriod:                     defaultRetentionPeriod,
		blockSize:                           defaultBlockSize,
		bufferFuture:                        defaultBufferFuture,
		bufferPast:                          defaultBufferPast,
		dataExpiry:                          defaultDataExpiry,
		dataExpiryAfterNotAccessedPeriod:    defaultDataExpiryAfterNotAccessedPeriod,
		nonRealtimeFlushAfterNoMetricPeriod: defaultNonRealtimeFlushAfterNoMetricPeriod,
		nonRealtimeMaxWritesBeforeFlush:     defaultNonRealtimeMaxWritesBeforeFlush,
		nonRealtimeWritesEnabled:            defaultNonRealtimeWritesEnabled,
	}
}

func (o *options) Validate() error {
	if o.bufferFuture < 0 {
		return errBufferFutureNonNegative
	}
	if o.bufferPast < 0 {
		return errBufferPastNonNegative
	}
	if o.blockSize <= 0 {
		return errBlockSizePositive
	}
	if o.bufferFuture >= o.blockSize {
		return errBufferFutureTooLarge
	}
	if o.bufferPast >= o.blockSize {
		return errBufferPastTooLarge
	}
	if o.retentionPeriod < o.blockSize {
		return errRetentionPeriodTooSmall
	}
	return nil
}

func (o *options) Equal(value Options) bool {
	return o.retentionPeriod == value.RetentionPeriod() &&
		o.blockSize == value.BlockSize() &&
		o.bufferFuture == value.BufferFuture() &&
		o.bufferPast == value.BufferPast() &&
		o.dataExpiry == value.BlockDataExpiry() &&
		o.dataExpiryAfterNotAccessedPeriod == value.BlockDataExpiryAfterNotAccessedPeriod() &&
		o.nonRealtimeWritesEnabled == value.NonRealtimeWritesEnabled() &&
		o.nonRealtimeFlushAfterNoMetricPeriod == value.NonRealtimeFlushAfterNoMetricPeriod() &&
		o.nonRealtimeMaxWritesBeforeFlush == value.NonRealtimeMaxWritesBeforeFlush()
}

func (o *options) SetRetentionPeriod(value time.Duration) Options {
	opts := *o
	opts.retentionPeriod = value
	return &opts
}

func (o *options) RetentionPeriod() time.Duration {
	return o.retentionPeriod
}

func (o *options) SetBlockSize(value time.Duration) Options {
	opts := *o
	opts.blockSize = value
	return &opts
}

func (o *options) BlockSize() time.Duration {
	return o.blockSize
}

func (o *options) SetBufferFuture(value time.Duration) Options {
	opts := *o
	opts.bufferFuture = value
	return &opts
}

func (o *options) BufferFuture() time.Duration {
	return o.bufferFuture
}

func (o *options) SetBufferPast(value time.Duration) Options {
	opts := *o
	opts.bufferPast = value
	return &opts
}

func (o *options) BufferPast() time.Duration {
	return o.bufferPast
}

func (o *options) SetBlockDataExpiry(value bool) Options {
	opts := *o
	opts.dataExpiry = value
	return &opts
}

func (o *options) BlockDataExpiry() bool {
	return o.dataExpiry
}

func (o *options) SetBlockDataExpiryAfterNotAccessedPeriod(value time.Duration) Options {
	opts := *o
	opts.dataExpiryAfterNotAccessedPeriod = value
	return &opts
}

func (o *options) BlockDataExpiryAfterNotAccessedPeriod() time.Duration {
	return o.dataExpiryAfterNotAccessedPeriod
}

func (o *options) SetNonRealtimeWritesEnabled(value bool) Options {
	opts := *o
	opts.nonRealtimeWritesEnabled = value
	return &opts
}

func (o *options) NonRealtimeWritesEnabled() bool {
	return o.nonRealtimeWritesEnabled
}

func (o *options) SetNonRealtimeFlushAfterNoMetricPeriod(value time.Duration) Options {
	opts := *o
	opts.nonRealtimeFlushAfterNoMetricPeriod = value
	return &opts
}

func (o *options) NonRealtimeFlushAfterNoMetricPeriod() time.Duration {
	return o.nonRealtimeFlushAfterNoMetricPeriod
}

func (o *options) SetNonRealtimeMaxWritesBeforeFlush(value uint64) Options {
	opts := *o
	opts.nonRealtimeMaxWritesBeforeFlush = value
	return &opts
}

func (o *options) NonRealtimeMaxWritesBeforeFlush() uint64 {
	return o.nonRealtimeMaxWritesBeforeFlush
}
