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

// Sample represents a sampled value
type Sample struct {
	value    float64 // sampled value
	numRanks int64   // number of ranks represented
	delta    int64   // delta between min rank and max rank
	prev     *Sample // previous sample
	next     *Sample // next sample
}

// SampleAllocate allocates a new sample for a pool
type SampleAllocate func() *Sample

// SamplePool is a pool of samples
type SamplePool interface {
	// Init initializes the pool
	Init()

	// Get returns a sample from the pool
	Get() *Sample

	// Put returns a sample to the pool
	Put(sample *Sample)
}

// FloatsPool provides a pool for variable-sized float64 slices
type FloatsPool interface {
	// Init initializes the pool
	Init()

	// Get provides an float64 slice from the pool
	Get(capacity int) []float64

	// Put returns an float64 slice to the pool
	Put(value []float64)
}

// Stream represents a data sample stream for floating point numbers
type Stream interface {
	// Add adds a sample value
	Add(value float64)

	// Flush flushes the internal buffer
	Flush()

	// Quantile returns the quantile value
	Quantile(q float64) float64

	// Close closes the stream
	Close()

	// Reset resets the stream
	Reset()
}

// Options represent various options for computing quantiles
type Options interface {
	// SetEps sets the desired epsilon for errors
	SetEps(value float64) Options

	// Eps returns the desired epsilon for errors
	Eps() float64

	// SetQuantiles sets the quantiles to be computed
	SetQuantiles(value []float64) Options

	// Quantiles returns the quantiles to be computed
	Quantiles() []float64

	// SetSamplePool sets the sample pool
	SetSamplePool(value SamplePool) Options

	// SamplePool returns the sample pool
	SamplePool() SamplePool

	// SetFloatsPool sets the floats pool
	SetFloatsPool(value FloatsPool) Options

	// FloatsPool returns the floats pool
	FloatsPool() FloatsPool

	// Validate validates the options
	Validate() error
}
