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

package cm

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/pool"
)

const (
	minEps          = 0.0
	maxEps          = 0.5
	defaultEps      = 1e-3
	defaultCapacity = 16

	// By default the timer values are inserted and the underlying
	// streams are compressed every time the value is added.
	defaultInsertAndCompressEvery = 1

	// By default the stream is not flushed when values are added.
	defaultFlushEvery = 0
)

var (
	defaultBuckets = []pool.Bucket{
		{Capacity: 16, Count: 4096},
	}

	errInvalidEps   = fmt.Errorf("epsilon value must be between %f and %f", minEps, maxEps)
	errNoFloatsPool = errors.New("no floats pool set")
	errNoStreamPool = errors.New("no stream pool set")
)

type options struct {
	eps                    float64
	capacity               int
	insertAndCompressEvery int
	flushEvery             int
	streamPool             StreamPool
	samplePool             SamplePool
	floatsPool             pool.FloatsPool
}

// NewOptions creates a new options.
func NewOptions() Options {
	o := &options{
		eps:                    defaultEps,
		capacity:               defaultCapacity,
		insertAndCompressEvery: defaultInsertAndCompressEvery,
		flushEvery:             defaultFlushEvery,
	}

	o.initPools()
	return o
}

func (o *options) SetEps(value float64) Options {
	opts := *o
	opts.eps = value
	return &opts
}

func (o *options) Eps() float64 {
	return o.eps
}

func (o *options) SetCapacity(value int) Options {
	opts := *o
	opts.capacity = value
	return &opts
}

func (o *options) Capacity() int {
	return o.capacity
}

func (o *options) SetInsertAndCompressEvery(value int) Options {
	opts := *o
	opts.insertAndCompressEvery = value
	return &opts
}

func (o *options) InsertAndCompressEvery() int {
	return o.insertAndCompressEvery
}

func (o *options) SetFlushEvery(value int) Options {
	opts := *o
	opts.flushEvery = value
	return &opts
}

func (o *options) FlushEvery() int {
	return o.flushEvery
}

func (o *options) SetStreamPool(value StreamPool) Options {
	opts := *o
	opts.streamPool = value
	return &opts
}

func (o *options) StreamPool() StreamPool {
	return o.streamPool
}

func (o *options) SetSamplePool(value SamplePool) Options {
	opts := *o
	opts.samplePool = value
	return &opts
}

func (o *options) SamplePool() SamplePool {
	return o.samplePool
}

func (o *options) SetFloatsPool(value pool.FloatsPool) Options {
	opts := *o
	opts.floatsPool = value
	return &opts
}

func (o *options) FloatsPool() pool.FloatsPool {
	return o.floatsPool
}

func (o *options) Validate() error {
	if o.eps <= minEps || o.eps >= maxEps {
		return errInvalidEps
	}
	if o.streamPool == nil {
		return errNoStreamPool
	}
	if o.floatsPool == nil {
		return errNoFloatsPool
	}
	return nil
}

func (o *options) initPools() {
	o.floatsPool = pool.NewFloatsPool(defaultBuckets, nil)
	o.floatsPool.Init()

	o.streamPool = NewStreamPool(nil)
	o.streamPool.Init(func() Stream { return NewStream(nil, o) })
}
