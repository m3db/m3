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

// Options represent various options for computing quantiles.
type Options interface {
	// SetEps sets the desired epsilon for errors.
	SetEps(value float64) Options

	// Eps returns the desired epsilon for errors.
	Eps() float64

	// SetCapacity sets the initial heap capacity.
	SetCapacity(value int) Options

	// Capacity returns the initial heap capacity.
	Capacity() int

	// SetInsertAndCompressEvery sets how frequently the timer values are
	// inserted into the stream and compressed to reduce write latency for
	// high frequency timers.
	SetInsertAndCompressEvery(value int) Options

	// InsertAndCompressEvery returns how frequently the timer values are
	// inserted into the stream and compressed to reduce write latency for
	// high frequency timers.
	InsertAndCompressEvery() int

	// SetStreamPool sets the stream pool.
	SetStreamPool(value StreamPool) Options

	// StreamPool returns the stream pool.
	StreamPool() StreamPool

	// Validate validates the options.
	Validate() error
}
