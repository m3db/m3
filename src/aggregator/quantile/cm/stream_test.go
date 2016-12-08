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
	"math"
	"math/rand"
	"testing"

	"github.com/m3db/m3aggregator/pool"
	"github.com/stretchr/testify/require"
)

func testStreamOptions() Options {
	return NewOptions().
		SetEps(0.01).
		SetQuantiles([]float64{0.5, 0.9, 0.99})
}

func TestEmptyStream(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts)
	for _, q := range opts.Quantiles() {
		require.Equal(t, 0.0, s.Quantile(q))
	}
}

func TestStreamWithOnePositiveSample(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts)
	s.Add(100.0)
	s.Flush()

	for _, q := range opts.Quantiles() {
		require.Equal(t, 100.0, s.Quantile(q))
	}
}

func TestStreamWithOneNegativeSample(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts)
	s.Add(-100.0)
	s.Flush()

	for _, q := range opts.Quantiles() {
		require.Equal(t, -100.0, s.Quantile(q))
	}
}

func TestStreamWithThreeSamples(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts)
	for _, val := range []float64{100.0, 200.0, 300.0} {
		s.Add(val)
	}
	s.Flush()

	expected := []float64{200.0, 300.0, 300.0}
	for i, q := range opts.Quantiles() {
		require.Equal(t, expected[i], s.Quantile(q))
	}
}

func TestStreamWithIncreasingSamples(t *testing.T) {
	numSamples := 100000
	opts := testStreamOptions()
	s := NewStream(0, opts)
	for i := 0; i < numSamples; i++ {
		s.Add(float64(i))
	}
	s.Flush()

	margin := float64(numSamples) * opts.Eps()
	for _, q := range opts.Quantiles() {
		val := s.Quantile(q)
		require.True(t, val >= float64(numSamples)*q-margin && val <= float64(numSamples)*q+margin)
	}
}

func TestStreamWithDecreasingSamples(t *testing.T) {
	numSamples := 100000
	opts := testStreamOptions()
	s := NewStream(0, opts)
	for i := numSamples - 1; i >= 0; i-- {
		s.Add(float64(i))
	}
	s.Flush()

	margin := float64(numSamples) * opts.Eps()
	for _, q := range opts.Quantiles() {
		val := s.Quantile(q)
		require.True(t, val >= float64(numSamples)*q-margin && val <= float64(numSamples)*q+margin)
	}
}

func TestStreamWithRandomSamples(t *testing.T) {
	numSamples := 100000
	maxInt64 := int64(math.MaxInt64)
	opts := testStreamOptions()
	s := NewStream(0, opts)

	rand.Seed(100)
	for i := 0; i < numSamples; i++ {
		s.Add(float64(rand.Int63n(maxInt64)))
	}
	s.Flush()

	margin := float64(maxInt64) * opts.Eps()
	for _, q := range opts.Quantiles() {
		val := s.Quantile(q)
		require.True(t, val >= float64(maxInt64)*q-margin && val <= float64(maxInt64)*q+margin)
	}
}

func TestStreamWithSkewedDistribution(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts)
	for i := 0; i < 10000; i++ {
		s.Add(1.0)
	}

	// Add a huge sample value (10M)
	s.Add(10000000.0)
	s.Flush()

	for _, q := range opts.Quantiles() {
		require.Equal(t, 1.0, s.Quantile(q))
	}
}

func TestStreamClose(t *testing.T) {
	opts := testStreamOptions()
	s := NewStream(0, opts).(*stream)
	require.False(t, s.closed)

	// Close the stream
	s.Close()
	require.True(t, s.closed)

	// Close the stream again, should be a no-op
	s.Close()
	require.True(t, s.closed)
}

func TestStreamAddToMinHeap(t *testing.T) {
	floatsPool := NewFloatsPool(pool.NewBucketizedObjectPoolOptions().SetBuckets(
		[]pool.Bucket{
			{Capacity: 1, Count: 1},
			{Capacity: 2, Count: 1},
		}))
	floatsPool.Init()
	opts := testStreamOptions().SetFloatsPool(floatsPool)
	s := NewStream(0, opts).(*stream)

	heap := minHeap(floatsPool.Get(1))
	require.Equal(t, 1, cap(heap))

	inputs := []float64{1.0, 2.0}

	// Push one value to the heap, still under capacity
	s.addToMinHeap(&heap, inputs[0])
	require.Equal(t, inputs[:1], []float64(heap))
	require.Equal(t, 1, cap(heap))

	// Push another value to the heap, which causes the capacity to grow
	s.addToMinHeap(&heap, inputs[1])
	require.Equal(t, inputs, []float64(heap))
	require.Equal(t, 2, cap(heap))
}
