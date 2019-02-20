// Copyright (c) 2018 Uber Technologies, Inc.
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
package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContextPoolPolicyPoolPolicy(t *testing.T) {
	size := 10
	refillLowWaterMark := 0.5
	refillHighWaterMark := 0.7
	cpp := ContextPoolPolicy{
		PoolPolicy: PoolPolicy{
			Size:                &size,
			RefillLowWaterMark:  &refillLowWaterMark,
			RefillHighWaterMark: &refillHighWaterMark,
		},
	}

	require.Equal(t, size, cpp.SizeOrDefault())
	require.Equal(t, refillLowWaterMark, cpp.RefillLowWaterMarkOrDefault())
	require.Equal(t, refillHighWaterMark, cpp.RefillHighWaterMarkOrDefault())
}

func TestContextPoolMaxFinalizerCapacityOrDefault(t *testing.T) {
	cpp := ContextPoolPolicy{
		MaxFinalizerCapacity: 0,
	}
	require.Equal(t, defaultMaxFinalizerCapacity, cpp.MaxFinalizerCapacityOrDefault())

	cpp.MaxFinalizerCapacity = 10
	require.Equal(t, 10, cpp.MaxFinalizerCapacityOrDefault())
}
