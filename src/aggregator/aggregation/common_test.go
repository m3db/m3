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

package aggregation

import (
	"testing"

	"github.com/m3db/m3/src/metrics/aggregation"

	"github.com/stretchr/testify/require"
)

func TestStdev(t *testing.T) {
	require.InDelta(t, 29.01149, stdev(100, 338350, 5050), 0.001)
}

func TestIsExpensive(t *testing.T) {
	require.False(t, isExpensive(aggregation.Types{}))
	require.True(t, isExpensive(aggregation.Types{aggregation.Sum, aggregation.SumSq}))
	require.True(t, isExpensive(aggregation.Types{aggregation.Stdev}))
	require.False(t, isExpensive(aggregation.Types{aggregation.Sum}))
	require.False(t, isExpensive(aggregation.Types{aggregation.Count, aggregation.P999}))
}
