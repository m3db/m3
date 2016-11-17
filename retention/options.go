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

	// defaultBufferDrain is the default buffer drain
	defaultBufferDrain = 2 * time.Minute

	// defaultDiskModeOn is the default bool for whether disk mode is on
	defaultDiskModeOn = false
)

type options struct {
	retentionPeriod     time.Duration
	blockSize           time.Duration
	bufferFuture        time.Duration
	bufferPast          time.Duration
	bufferDrain         time.Duration
	diskModeOn          bool
	diskModeInMemPeriod time.Duration
}

// NewOptions creates new retention options
func NewOptions() Options {
	return &options{
		retentionPeriod: defaultRetentionPeriod,
		blockSize:       defaultBlockSize,
		bufferFuture:    defaultBufferFuture,
		bufferPast:      defaultBufferPast,
		bufferDrain:     defaultBufferDrain,
		diskModeOn:      defaultDiskModeOn,
	}
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

func (o *options) SetBufferDrain(value time.Duration) Options {
	opts := *o
	opts.bufferDrain = value
	return &opts
}

func (o *options) BufferDrain() time.Duration {
	return o.bufferDrain
}

func (o *options) SetDiskMode(diskModeOn bool) Options {
	opts := *o
	opts.diskModeOn = diskModeOn
	return &opts
}

func (o *options) DiskMode() bool {
	return o.diskModeOn
}

func (o *options) SetDiskModeInMemPeriod(period time.Duration) Options {
	opts := *o
	opts.diskModeInMemPeriod = period
	return &opts
}

func (o *options) DiskModeInMemPeriod() time.Duration {
	return o.diskModeInMemPeriod
}
