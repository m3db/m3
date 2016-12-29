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

// Centroid represents the center of a cluster
type Centroid struct {
	Mean   float64
	Weight float64
}

// CentroidsPool provides a pool for variable-sized centroid slices
type CentroidsPool interface {
	// Init initializes the pool
	Init()

	// Get provides a centroid slice from the pool
	Get(capacity int) []Centroid

	// Put returns a centroid slice to the pool
	Put(value []Centroid)
}

// TDigest is the t-digest interface
type TDigest interface {

	// Merged returns the merged centroids
	Merged() []Centroid

	// Unmerged returns the unmerged centroids
	Unmerged() []Centroid

	// Add adds a value
	Add(value float64)

	// Min returns the minimum value
	Min() float64

	// Max returns the maximum value
	Max() float64

	// Quantile returns the quantile value
	Quantile(q float64) float64

	// Merge merges another t-digest
	Merge(tdigest TDigest)

	// Close clsoes the t-digest
	Close()

	// Reset resets the t-digest
	Reset()
}

// Options provides a set of t-digest options
type Options interface {
	// SetCompression sets the compression
	SetCompression(value float64) Options

	// Compression returns the compression
	Compression() float64

	// SetPrecision sets the quantile precision
	SetPrecision(value int) Options

	// Precision returns the quantile precision
	Precision() int

	// SetCentroidsPool sets the centroids pool
	SetCentroidsPool(value CentroidsPool) Options

	// CentroidsPool returns the centroids pool
	CentroidsPool() CentroidsPool

	// Validate validates the options
	Validate() error
}
