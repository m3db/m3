// Copyright (c) 2018 Uber Technologies, Inc.
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

package buffer

import (
	"errors"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
)

const (
	defaultMaxBufferSize         = 100 * 1024 * 1024 // 100MB.
	defaultMaxMessageSize        = 1 * 1024 * 1024   // 1MB.
	defaultCloseCheckInterval    = time.Second
	defaultDropOldestInterval    = time.Second
	defaultScanBatchSize         = 16
	defaultCleanupInitialBackoff = 10 * time.Second
	defaultAllowedSpilloverRatio = 0.2
	defaultCleanupMaxBackoff     = time.Minute
)

var (
	errInvalidScanBatchSize   = errors.New("invalid scan batch size")
	errInvalidMaxMessageSize  = errors.New("invalid max message size")
	errNegativeMaxBufferSize  = errors.New("negative max buffer size")
	errNegativeMaxMessageSize = errors.New("negative max message size")
)

type bufferOptions struct {
	strategy              OnFullStrategy
	maxBufferSize         int
	maxMessageSize        int
	closeCheckInterval    time.Duration
	dropOldestInterval    time.Duration
	scanBatchSize         int
	allowedSpilloverRatio float64
	rOpts                 retry.Options
	iOpts                 instrument.Options
}

// NewOptions creates Options.
func NewOptions() Options {
	return &bufferOptions{
		strategy:              DropOldest,
		maxBufferSize:         defaultMaxBufferSize,
		maxMessageSize:        defaultMaxMessageSize,
		closeCheckInterval:    defaultCloseCheckInterval,
		dropOldestInterval:    defaultDropOldestInterval,
		scanBatchSize:         defaultScanBatchSize,
		allowedSpilloverRatio: defaultAllowedSpilloverRatio,
		rOpts: retry.NewOptions().
			SetInitialBackoff(defaultCleanupInitialBackoff).
			SetMaxBackoff(defaultCleanupMaxBackoff).
			SetForever(true),
		iOpts: instrument.NewOptions(),
	}
}

func (opts *bufferOptions) OnFullStrategy() OnFullStrategy {
	return opts.strategy
}

func (opts *bufferOptions) SetOnFullStrategy(value OnFullStrategy) Options {
	o := *opts
	o.strategy = value
	return &o
}

func (opts *bufferOptions) MaxMessageSize() int {
	return opts.maxMessageSize
}

func (opts *bufferOptions) SetMaxMessageSize(value int) Options {
	o := *opts
	o.maxMessageSize = value
	return &o
}

func (opts *bufferOptions) MaxBufferSize() int {
	return opts.maxBufferSize
}

func (opts *bufferOptions) SetMaxBufferSize(value int) Options {
	o := *opts
	o.maxBufferSize = value
	return &o
}

func (opts *bufferOptions) CloseCheckInterval() time.Duration {
	return opts.closeCheckInterval
}

func (opts *bufferOptions) SetCloseCheckInterval(value time.Duration) Options {
	o := *opts
	o.closeCheckInterval = value
	return &o
}

func (opts *bufferOptions) DropOldestInterval() time.Duration {
	return opts.dropOldestInterval
}

func (opts *bufferOptions) SetDropOldestInterval(value time.Duration) Options {
	o := *opts
	o.dropOldestInterval = value
	return &o
}

func (opts *bufferOptions) ScanBatchSize() int {
	return opts.scanBatchSize
}

func (opts *bufferOptions) SetScanBatchSize(value int) Options {
	o := *opts
	o.scanBatchSize = value
	return &o
}

func (opts *bufferOptions) AllowedSpilloverRatio() float64 {
	return opts.allowedSpilloverRatio
}

func (opts *bufferOptions) SetAllowedSpilloverRatio(value float64) Options {
	o := *opts
	o.allowedSpilloverRatio = value
	return &o
}

func (opts *bufferOptions) CleanupRetryOptions() retry.Options {
	return opts.rOpts
}

func (opts *bufferOptions) SetCleanupRetryOptions(value retry.Options) Options {
	o := *opts
	o.rOpts = value
	return &o
}

func (opts *bufferOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *bufferOptions) SetInstrumentOptions(value instrument.Options) Options {
	o := *opts
	o.iOpts = value
	return &o
}

func (opts *bufferOptions) Validate() error {
	if opts.ScanBatchSize() <= 0 {
		return errInvalidScanBatchSize
	}
	if opts.MaxBufferSize() <= 0 {
		return errNegativeMaxBufferSize
	}
	if opts.MaxMessageSize() <= 0 {
		return errNegativeMaxMessageSize
	}
	if opts.MaxMessageSize() > opts.MaxBufferSize() {
		// Max message size can only be as large as max buffer size.
		return errInvalidMaxMessageSize
	}
	return nil
}
