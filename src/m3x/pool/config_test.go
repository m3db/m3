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

package pool

import (
	"testing"

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestObjectPoolConfiguration(t *testing.T) {
	cfg := ObjectPoolConfiguration{
		Size: 1,
		WaterMark: WaterMarkConfiguration{
			RefillLowWaterMark:  0.1,
			RefillHighWaterMark: 0.5,
		},
	}
	opts := cfg.NewObjectPoolOptions(instrument.NewOptions()).(*objectPoolOptions)
	require.Equal(t, 1, opts.size)
	require.Equal(t, 0.1, opts.refillLowWatermark)
	require.Equal(t, 0.5, opts.refillHighWatermark)
}

func TestBucketizedPoolConfiguration(t *testing.T) {
	cfg := BucketizedPoolConfiguration{
		Buckets: []BucketConfiguration{
			{Count: 1, Capacity: 10},
			{Count: 2, Capacity: 20},
		},
		WaterMark: WaterMarkConfiguration{
			RefillLowWaterMark:  0.1,
			RefillHighWaterMark: 0.5,
		},
	}
	expectedBuckets := []Bucket{
		{Count: 1, Capacity: 10},
		{Count: 2, Capacity: 20},
	}
	require.Equal(t, expectedBuckets, cfg.NewBuckets())
	opts := cfg.NewObjectPoolOptions(instrument.NewOptions()).(*objectPoolOptions)
	require.Equal(t, 0.1, opts.refillLowWatermark)
	require.Equal(t, 0.5, opts.refillHighWatermark)
}
