// Copyright (c) 2020 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package buffer

import (
	"fmt"
	"time"
)

const (
	// 4mb total for fixed buffer.
	defaultBufferCount    = 64
	defaultBufferCapacity = 65536
)

type options struct {
	bufferCount    int
	bufferCapacity int
	bufferTimeout  time.Duration
}

// NewOptions creates a new set of wide options.
func NewOptions() Options {
	return &options{
		bufferCount:    defaultBufferCount,
		bufferCapacity: defaultBufferCapacity,
	}
}

func (o *options) Validate() error {
	if o.bufferCapacity <= 0 {
		return fmt.Errorf("buffer capacity %d must be greater than 0", o.bufferCapacity)
	}

	if o.bufferCount <= 0 {
		return fmt.Errorf("buffer size %d must be greater than 0", o.bufferCount)
	}

	if o.bufferTimeout < 0 {
		return fmt.Errorf("timeout %v must be non-negative", o.bufferTimeout)
	}

	return nil
}

func (o *options) SetFixedBufferCount(value int) Options {
	opts := *o
	opts.bufferCount = value
	return &opts
}

func (o *options) FixedBufferCount() int {
	return o.bufferCount
}

func (o *options) SetFixedBufferCapacity(value int) Options {
	opts := *o
	opts.bufferCapacity = value
	return &opts
}

func (o *options) FixedBufferCapacity() int {
	return o.bufferCapacity
}

func (o *options) SetFixedBufferTimeout(value time.Duration) Options {
	opts := *o
	opts.bufferTimeout = value
	return &opts
}

func (o *options) FixedBufferTimeout() time.Duration {
	return o.bufferTimeout
}
