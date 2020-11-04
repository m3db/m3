// Copyright (c) 2020 Uber Technologies, Inc.
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

package bootstrapper

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/stretchr/testify/require"
)

func TestIntersectingShardTimeRanges(t *testing.T) {
	shards := []uint32{0, 1, 2, 3}
	blockSize := time.Hour
	t0 := time.Now().Truncate(blockSize)
	t1 := t0.Add(blockSize)
	t2 := t1.Add(blockSize)
	fullRange := result.NewShardTimeRangesFromRange(t0, t2, shards...)

	expectedIntersect := result.NewShardTimeRangesFromRange(t1, t2, shards...)
	intersect := IntersectingShardTimeRanges(fullRange, shards, t1, blockSize)
	require.True(t, intersect.Equal(expectedIntersect))

	// Try with non-overlapping shards.
	intersectShards := []uint32{0}
	expectedIntersect = result.NewShardTimeRangesFromRange(t1, t2, intersectShards...)
	intersect = IntersectingShardTimeRanges(fullRange, intersectShards, t1, blockSize)
	require.True(t, intersect.Equal(expectedIntersect))
}
