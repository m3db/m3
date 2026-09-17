// Copyright (c) 2024 Uber Technologies, Inc.
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

package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTickResult(t *testing.T) {
	t.Run("EmptyMerge", func(t *testing.T) {
		r1 := tickResult{}
		r2 := tickResult{}
		merged := r1.merge(r2)
		require.Equal(t, tickResult{}, merged)
	})

	t.Run("SingleValueMerge", func(t *testing.T) {
		r1 := tickResult{activeSeries: 1}
		r2 := tickResult{activeSeries: 2}
		merged := r1.merge(r2)
		require.Equal(t, 3, merged.activeSeries)
	})

	t.Run("MultipleValuesMerge", func(t *testing.T) {
		r1 := tickResult{
			activeSeries:           1,
			expiredSeries:          2,
			activeBlocks:           3,
			wiredBlocks:            4,
			unwiredBlocks:          5,
			pendingMergeBlocks:     6,
			madeExpiredBlocks:      7,
			madeUnwiredBlocks:      8,
			mergedOutOfOrderBlocks: 9,
			errors:                 10,
			evictedBuckets:         11,
		}
		r2 := tickResult{
			activeSeries:           10,
			expiredSeries:          20,
			activeBlocks:           30,
			wiredBlocks:            40,
			unwiredBlocks:          50,
			pendingMergeBlocks:     60,
			madeExpiredBlocks:      70,
			madeUnwiredBlocks:      80,
			mergedOutOfOrderBlocks: 90,
			errors:                 100,
			evictedBuckets:         110,
		}
		merged := r1.merge(r2)
		require.Equal(t, 11, merged.activeSeries)
		require.Equal(t, 22, merged.expiredSeries)
		require.Equal(t, 33, merged.activeBlocks)
		require.Equal(t, 44, merged.wiredBlocks)
		require.Equal(t, 55, merged.unwiredBlocks)
		require.Equal(t, 66, merged.pendingMergeBlocks)
		require.Equal(t, 77, merged.madeExpiredBlocks)
		require.Equal(t, 88, merged.madeUnwiredBlocks)
		require.Equal(t, 99, merged.mergedOutOfOrderBlocks)
		require.Equal(t, 110, merged.errors)
		require.Equal(t, 121, merged.evictedBuckets)
	})

	t.Run("NegativeValuesMerge", func(t *testing.T) {
		r1 := tickResult{activeSeries: -1}
		r2 := tickResult{activeSeries: -2}
		merged := r1.merge(r2)
		require.Equal(t, -3, merged.activeSeries)
	})

	t.Run("MixedValuesMerge", func(t *testing.T) {
		r1 := tickResult{
			activeSeries:  1,
			expiredSeries: -2,
			activeBlocks:  3,
			wiredBlocks:   -4,
			unwiredBlocks: 5,
		}
		r2 := tickResult{
			activeSeries:  -1,
			expiredSeries: 2,
			activeBlocks:  -3,
			wiredBlocks:   4,
			unwiredBlocks: -5,
		}
		merged := r1.merge(r2)
		require.Equal(t, 0, merged.activeSeries)
		require.Equal(t, 0, merged.expiredSeries)
		require.Equal(t, 0, merged.activeBlocks)
		require.Equal(t, 0, merged.wiredBlocks)
		require.Equal(t, 0, merged.unwiredBlocks)
	})

	t.Run("LargeValuesMerge", func(t *testing.T) {
		r1 := tickResult{activeSeries: 1000000}
		r2 := tickResult{activeSeries: 2000000}
		merged := r1.merge(r2)
		require.Equal(t, 3000000, merged.activeSeries)
	})

	t.Run("ZeroValueMerge", func(t *testing.T) {
		r1 := tickResult{activeSeries: 0}
		r2 := tickResult{activeSeries: 0}
		merged := r1.merge(r2)
		require.Equal(t, 0, merged.activeSeries)
	})
}

func TestResult(t *testing.T) {
	t.Run("ResultOperations", func(t *testing.T) {
		result := &tickResult{
			activeSeries:  1,
			expiredSeries: -2,
			activeBlocks:  3,
			wiredBlocks:   -4,
			unwiredBlocks: 5,
		}

		require.Equal(t, 1, result.activeSeries)
		require.Equal(t, -2, result.expiredSeries)
		require.Equal(t, 3, result.activeBlocks)
		require.Equal(t, -4, result.wiredBlocks)
		require.Equal(t, 5, result.unwiredBlocks)

		result = &tickResult{
			activeSeries:  -1,
			expiredSeries: 2,
			activeBlocks:  -3,
			wiredBlocks:   4,
			unwiredBlocks: -5,
		}

		require.Equal(t, -1, result.activeSeries)
		require.Equal(t, 2, result.expiredSeries)
		require.Equal(t, -3, result.activeBlocks)
		require.Equal(t, 4, result.wiredBlocks)
		require.Equal(t, -5, result.unwiredBlocks)
	})
}
