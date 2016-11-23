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
