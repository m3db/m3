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
	"math"
	"math/rand"
	"testing"

	"github.com/m3db/m3aggregator/pool"
	"github.com/stretchr/testify/require"
)

var (
	testQuantiles = []float64{0.5, 0.95, 0.99}
)

func testTDigestOptions() Options {
	return NewOptions()
}

func TestEmptyTDigest(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	for _, q := range testQuantiles {
		require.True(t, math.IsNaN(d.Quantile(q)))
	}
}

func TestTDigestWithOutOfBoundsQuantile(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	require.True(t, math.IsNaN(d.Quantile(-1.0)))
	require.True(t, math.IsNaN(d.Quantile(10.0)))
}

func TestTDigestWithOneValue(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	d.Add(100.0)
	for _, q := range testQuantiles {
		require.Equal(t, 100.0, d.Quantile(q))
	}
}

func TestTDigestWithThreeValues(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	for _, val := range []float64{100.0, 200.0, 300.0} {
		d.Add(val)
	}

	expected := []float64{200.0, 300.0, 300.0}
	for i, q := range testQuantiles {
		require.InEpsilon(t, expected[i], d.Quantile(q), 0.025)
	}
}

func TestTDigestWithIncreasingValues(t *testing.T) {
	numSamples := 100000
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	for i := 0; i < numSamples; i++ {
		d.Add(float64(i))
	}

	for _, q := range testQuantiles {
		require.InEpsilon(t, float64(numSamples)*q, d.Quantile(q), 0.01)
	}
}

func TestTDigestWithDecreasingValues(t *testing.T) {
	numSamples := 100000
	opts := testTDigestOptions()
	d := NewTDigest(opts)
	for i := numSamples - 1; i >= 0; i-- {
		d.Add(float64(i))
	}

	for _, q := range testQuantiles {
		require.InEpsilon(t, float64(numSamples)*q, d.Quantile(q), 0.01)
	}
}

func TestTDigestWithRandomValues(t *testing.T) {
	numSamples := 100000
	maxInt64 := int64(math.MaxInt64)
	opts := testTDigestOptions()
	d := NewTDigest(opts)

	rand.Seed(100)
	for i := 0; i < numSamples; i++ {
		d.Add(float64(rand.Int63n(maxInt64)))
	}

	for _, q := range testQuantiles {
		require.InEpsilon(t, float64(maxInt64)*q, d.Quantile(q), 0.01)
	}
}

func TestTDigestMerge(t *testing.T) {
	var (
		numSamples = 100000
		numMerges  = 10
		maxInt64   = int64(math.MaxInt64)
		opts       = testTDigestOptions()
		digests    = make([]TDigest, 0, numMerges)
	)
	for i := 0; i < numMerges; i++ {
		digests = append(digests, NewTDigest(opts))
	}

	rand.Seed(100)
	for i := 0; i < numSamples; i++ {
		val := float64(rand.Int63n(maxInt64))
		digests[i%numMerges].Add(val)
	}

	merged := NewTDigest(opts)
	for i := 0; i < numMerges; i++ {
		merged.Merge(digests[i])
	}

	for _, q := range testQuantiles {
		require.InEpsilon(t, float64(maxInt64)*q, merged.Quantile(q), 0.01)
	}
}

func TestTDigestClose(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts).(*tDigest)
	require.False(t, d.closed)

	// Close the t-digest
	d.Close()
	require.True(t, d.closed)

	// Close the t-digest again, should be a no-op
	d.Close()
	require.True(t, d.closed)
}

func TestTDigestAppendCentroid(t *testing.T) {
	centroidsPool := NewCentroidsPool(pool.NewBucketizedObjectPoolOptions().SetBuckets(
		[]pool.Bucket{
			{Capacity: 1, Count: 1},
			{Capacity: 2, Count: 1},
		}))
	centroidsPool.Init()
	opts := testTDigestOptions().SetCentroidsPool(centroidsPool)
	d := NewTDigest(opts).(*tDigest)

	centroids := centroidsPool.Get(1)
	require.Equal(t, 1, cap(centroids))

	inputs := []Centroid{
		{Mean: 1.0, Weight: 0.5},
		{Mean: 2.0, Weight: 0.8},
	}

	// Append one centroid, still under capacity
	centroids = d.appendCentroid(centroids, inputs[0])
	require.Equal(t, []Centroid{inputs[0]}, centroids)
	require.Equal(t, 1, cap(centroids))

	// Append another centroid, which causes the capacity to grow
	centroids = d.appendCentroid(centroids, inputs[1])
	require.Equal(t, inputs, centroids)
	require.Equal(t, 2, cap(centroids))
}

func TestTDigestCompress(t *testing.T) {
	opts := testTDigestOptions()
	d := NewTDigest(opts).(*tDigest)

	var result []Centroid
	d.mergeCentroidFn = func(
		currIndex float64,
		currWeight float64,
		totalWeight float64,
		c Centroid,
		mergeResult []Centroid,
	) (float64, []Centroid) {
		result = append(result, c)
		return currIndex + 1, result
	}
	d.merged = []Centroid{
		{Mean: 1.0, Weight: 0.5},
		{Mean: 2.0, Weight: 0.8},
	}
	d.mergedWeight = 1.3
	d.unmerged = []Centroid{
		{Mean: 5.0, Weight: 1.2},
		{Mean: 4.0, Weight: 2.0},
		{Mean: 6.0, Weight: 1.5},
	}
	d.unmergedWeight = 4.7

	d.compress()

	expected := []Centroid{
		{Mean: 1.0, Weight: 0.5},
		{Mean: 2.0, Weight: 0.8},
		{Mean: 4.0, Weight: 2.0},
		{Mean: 5.0, Weight: 1.2},
		{Mean: 6.0, Weight: 1.5},
	}
	require.Equal(t, expected, d.merged)
	require.Equal(t, 6.0, d.mergedWeight)
	require.Equal(t, 0, len(d.unmerged))
	require.Equal(t, 0.0, d.unmergedWeight)
}
