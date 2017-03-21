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

package datums

// TSGenFn represents a pure function used to
// generate synthetic time series'
type TSGenFn func(idx int) float64

// SyntheticTimeSeries represents a synthetically generated
// time series
type SyntheticTimeSeries interface {
	// ID returns the id of the SyntheticTimeSeries
	ID() int

	// Size returns the number of points in the SyntheticTimeSeries
	Size() int

	// Data returns data points comprising the SyntheticTimeSeries
	Data() []float64

	// Get(n) returns the nth (circularly wrapped) data point
	Get(n int) float64

	// Next simulates an infinite iterator on the SyntheticTimeSeries
	Next() float64
}

// Registry is a collection of synthetic time series'
type Registry interface {
	// Get(n) returns the nth (wrapped circularly) SyntheticTimeSeries
	// known to the Registry.
	Get(n int) SyntheticTimeSeries

	// Size returns the number of unique time series'
	// the Registry is capable of generating.
	Size() int
}
