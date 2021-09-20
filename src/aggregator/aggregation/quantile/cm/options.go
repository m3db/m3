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
	"fmt"
)

const (
	minEps                        = 0.0
	maxEps                        = 0.5
	defaultEps                    = 1e-3
	defaultCapacity               = 32
	defaultInsertAndCompressEvery = 1024
)

var (
	errInvalidEps = fmt.Errorf("epsilon value must be between %f and %f", minEps, maxEps)
)

type options struct {
	streamPool             StreamPool
	eps                    float64
	capacity               int
	insertAndCompressEvery int
}

// NewOptions creates a new options.
func NewOptions() Options {
	o := &options{
		eps:                    defaultEps,
		capacity:               defaultCapacity,
		insertAndCompressEvery: defaultInsertAndCompressEvery,
	}
	o.streamPool = NewStreamPool(o)
	return o
}

func (o *options) SetEps(value float64) Options {
	o.eps = value
	return o
}

func (o *options) Eps() float64 {
	return o.eps
}

func (o *options) SetCapacity(value int) Options {
	o.capacity = value
	return o
}

func (o *options) Capacity() int {
	return o.capacity
}

func (o *options) SetInsertAndCompressEvery(value int) Options {
	o.insertAndCompressEvery = value
	return o
}

func (o *options) InsertAndCompressEvery() int {
	return o.insertAndCompressEvery
}

func (o *options) SetStreamPool(value StreamPool) Options {
	o.streamPool = value
	return o
}

func (o *options) StreamPool() StreamPool {
	return o.streamPool
}

func (o *options) Validate() error {
	if o.eps <= minEps || o.eps >= maxEps {
		return errInvalidEps
	}

	return nil
}
