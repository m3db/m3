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
	"time"

	"github.com/m3db/m3x/instrument"
)

var (
	defaultMaxBufferSize      = 1 * 1024 * 1024 // 1MB.
	defaultCleanupInterval    = time.Second
	defaultCloseCheckInterval = time.Second
)

type bufferOptions struct {
	strategy           OnFullStrategy
	maxBufferSize      int
	cleanupInterval    time.Duration
	closeCheckInterval time.Duration
	iOpts              instrument.Options
}

// NewBufferOptions creates a BufferOptions.
func NewBufferOptions() Options {
	return &bufferOptions{
		strategy:           DropEarliest,
		maxBufferSize:      defaultMaxBufferSize,
		cleanupInterval:    defaultCleanupInterval,
		closeCheckInterval: defaultCloseCheckInterval,
		iOpts:              instrument.NewOptions(),
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

func (opts *bufferOptions) MaxBufferSize() int {
	return opts.maxBufferSize
}

func (opts *bufferOptions) SetMaxBufferSize(value int) Options {
	o := *opts
	o.maxBufferSize = value
	return &o
}

func (opts *bufferOptions) CleanupInterval() time.Duration {
	return opts.cleanupInterval
}

func (opts *bufferOptions) SetCleanupInterval(value time.Duration) Options {
	o := *opts
	o.cleanupInterval = value
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

func (opts *bufferOptions) InstrumentOptions() instrument.Options {
	return opts.iOpts
}

func (opts *bufferOptions) SetInstrumentOptions(value instrument.Options) Options {
	o := *opts
	o.iOpts = value
	return &o
}
