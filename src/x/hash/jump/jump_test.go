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

package jump

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestHashDistribution tests that Hash is distributing keys evenly among buckets
func TestHashDistribution(t *testing.T) {
	// 95th percentile of Chi-Squared Distribution with 9, 99, 999, 9999 degrees of freedom
	chiSquared95Percentiles := []float64{16.9190, 123.2252, 1073.6427, 10232.7373}

	numBuckets := []int64{10, 100, 1000, 10000}
	for i, n := range numBuckets {
		totalKeys := uint64(n) * 1000
		stepSize := math.MaxUint64 / totalKeys
		buckets := make(map[int64]int, n)

		var hash, j uint64
		for ; j < totalKeys; j++ {
			idx := Hash(hash, n)
			buckets[idx]++
			hash += stepSize
		}

		expected := float64(totalKeys) / float64(n)
		var testStatistic float64
		for _, observed := range buckets {
			diff := float64(observed) - expected
			testStatistic += diff * diff / expected
		}

		// To test that Hash is evenly distributing keys among the buckets we'll perform Pearson's
		// chi-squared test with a significance level of 95%.
		assert.True(t, testStatistic < chiSquared95Percentiles[i])
	}
}

// TestHashMoved tests that Hash only redistributes approximately 1/(n+1) keys when going from
// n to n+1 buckets
func TestHashMoved(t *testing.T) {
	numBuckets := []int64{10, 100, 1000}

	for _, n := range numBuckets {
		totalKeys := uint64(n) * 1000
		stepSize := math.MaxUint64 / totalKeys
		oldBuckets := make(map[uint64]int64, totalKeys)

		var hash, j uint64
		for ; j < totalKeys; j++ {
			idx := Hash(hash, n)
			oldBuckets[hash] = idx
			hash += stepSize
		}

		newBuckets := make(map[uint64]int64, totalKeys)

		hash, j = 0, 0
		for ; j < totalKeys; j++ {
			idx := Hash(hash, n+1)
			newBuckets[hash] = idx
			hash += stepSize
		}

		var numMoved int
		hash = 0
		for ; j < totalKeys; j++ {
			if oldBuckets[hash] != newBuckets[hash] {
				numMoved++
			}
			hash += stepSize
		}

		movedPercent := float64(numMoved) / float64(totalKeys)
		idealPercent := 1.0 / float64(n+1)

		// To test that Hash is redistributing approximately the ideal number of keys we require that
		// the percentage of moved key is less than 1.5 times the ideal percentage of moved keys
		assert.True(t, movedPercent < 1.5*idealPercent)
	}
}

func TestHashBadInput(t *testing.T) {
	expected := int64(-1)
	actual := Hash(21, -1)
	assert.Equal(t, expected, actual)
}

func BenchmarkHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Hash(42, 132)
	}
}
