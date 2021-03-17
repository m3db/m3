// Copyright (c) 2021 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeapPool(t *testing.T) {
	h := sharedHeapPool.Get(_initialHeapBucketSize)
	require.NotNil(t, h)
	assert.Equal(t, _initialHeapBucketSize, cap(*h))
	assert.Equal(t, 0, len(*h))
	h.Reset()

	h2 := sharedHeapPool.Get(_initialHeapBucketSize + 1) // should fall into different bucket
	require.NotNil(t, h2)
	require.True(t, h != h2)
	assert.Equal(t, _initialHeapBucketSize*_heapSizeBucketGrowthFactor, cap(*h2))
	assert.Equal(t, 0, len(*h2))
	h2.Reset()

	h3 := sharedHeapPool.Get(65) // should get the next largest one
	require.NotNil(t, h3)
	require.True(t, h3 != h)
	require.True(t, h3 != h2)
	assert.Equal(t, 256, cap(*h3))
	assert.Equal(t, 0, len(*h3))
}
