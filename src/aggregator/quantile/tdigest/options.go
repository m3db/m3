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

package tdigest

import (
	"errors"
	"fmt"

	"github.com/m3db/m3x/pool"
)

const (
	defaultCompression = 100.0
	defaultPrecision   = 0
	minPrecision       = 0
	maxPrecision       = 16
	minCompression     = 20.0
	maxCompression     = 1000.0
)

var (
	defaultBuckets = []pool.Bucket{
		{Capacity: 16, Count: 4096},
	}

	errInvalidCompression = fmt.Errorf("compression must be between %f and %f", minCompression, maxCompression)
	errInvalidPrecision   = fmt.Errorf("precision must be between %d and %d", minPrecision, maxPrecision)
	errNoCentroidsPool    = errors.New("no centroids pool set")
)

type options struct {
	compression   float64
	precision     int
	centroidsPool CentroidsPool
}

// NewOptions creates a new options
func NewOptions() Options {
	centroidsPool := NewCentroidsPool(defaultBuckets, nil)
	centroidsPool.Init()

	return options{
		compression:   defaultCompression,
		precision:     defaultPrecision,
		centroidsPool: centroidsPool,
	}
}

func (o options) SetCompression(value float64) Options {
	o.compression = value
	return o
}

func (o options) Compression() float64 {
	return o.compression
}

func (o options) SetPrecision(value int) Options {
	o.precision = value
	return o
}

func (o options) Precision() int {
	return o.precision
}

func (o options) SetCentroidsPool(value CentroidsPool) Options {
	o.centroidsPool = value
	return o
}

func (o options) CentroidsPool() CentroidsPool {
	return o.centroidsPool
}

func (o options) Validate() error {
	if o.compression < minCompression || o.compression > maxCompression {
		return errInvalidCompression
	}
	if o.precision < minPrecision || o.precision > maxPrecision {
		return errInvalidPrecision
	}
	if o.centroidsPool == nil {
		return errNoCentroidsPool
	}
	return nil
}
