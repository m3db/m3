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

const (
	defaultCompression = 100.0
	defaultPrecision   = 0
)

type options struct {
	compression   float64
	precision     int
	centroidsPool CentroidsPool
}

// NewOptions creates a new options
func NewOptions() Options {
	centroidsPool := NewCentroidsPool(nil)
	centroidsPool.Init()

	return options{
		compression:   defaultCompression,
		precision:     defaultPrecision,
		centroidsPool: centroidsPool,
	}
}

func (o options) SetCompressionFactor(value float64) Options {
	o.compression = value
	return o
}

func (o options) CompressionFactor() float64 {
	return o.compression
}

func (o options) SetQuantilePrecision(value int) Options {
	o.precision = value
	return o
}

func (o options) QuantilePrecision() int {
	return o.precision
}

func (o options) SetCentroidsPool(value CentroidsPool) Options {
	o.centroidsPool = value
	return o
}

func (o options) CentroidsPool() CentroidsPool {
	return o.centroidsPool
}
