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

package mem

import (
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultInitialCapacity = 1024 * 1024
)

type opts struct {
	iopts           instrument.Options
	postingsPool    segment.PostingsListPool
	initialCapacity int
}

// NewOptions returns new options.
func NewOptions() Options {
	return &opts{
		iopts:           instrument.NewOptions(),
		postingsPool:    segment.NewPostingsListPool(nil, segment.NewPostingsList),
		initialCapacity: defaultInitialCapacity,
	}
}

func (o *opts) SetInstrumentOptions(v instrument.Options) Options {
	opts := *o
	opts.iopts = v
	return &opts
}

func (o *opts) InstrumentOptions() instrument.Options {
	return o.iopts
}

func (o *opts) SetPostingsListPool(v segment.PostingsListPool) Options {
	opts := *o
	opts.postingsPool = v
	return &opts
}

func (o *opts) PostingsListPool() segment.PostingsListPool {
	return o.postingsPool
}

func (o *opts) SetInitialCapacity(v int) Options {
	opts := *o
	opts.initialCapacity = v
	return &opts
}

func (o *opts) InitialCapacity() int {
	return o.initialCapacity
}
