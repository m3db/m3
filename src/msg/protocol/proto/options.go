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

package proto

import (
	"github.com/m3db/m3x/pool"
)

var (
	defaultMaxMessageSize = 4 * 1024 * 1024 // 4MB.
)

// NewOptions creates a new Options.
func NewOptions() Options {
	return &options{
		maxMessageSize: defaultMaxMessageSize,
	}
}

type options struct {
	maxMessageSize int
	bytesPool      pool.BytesPool
}

func (opts *options) MaxMessageSize() int {
	return opts.maxMessageSize
}

func (opts *options) SetMaxMessageSize(value int) Options {
	o := *opts
	o.maxMessageSize = value
	return &o
}

func (opts *options) BytesPool() pool.BytesPool {
	return opts.bytesPool
}

func (opts *options) SetBytesPool(value pool.BytesPool) Options {
	o := *opts
	o.bytesPool = value
	return &o
}
