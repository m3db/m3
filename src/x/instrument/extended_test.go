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

package instrument

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestDetailedExtendedMetrics(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	metrics := newRuntimeMetrics(DetailedExtendedMetrics, scope)
	metrics.report(DetailedExtendedMetrics)

	snapshot := scope.Snapshot()
	totalAlloc := snapshot.Counters()["test.runtime.memory.total-allocated+"]
	require.NotNil(t, totalAlloc)
	require.True(t, totalAlloc.Value() > 0)

	metrics.report(DetailedExtendedMetrics)
	snapshot = scope.Snapshot()
	totalAlloc2 := snapshot.Counters()["test.runtime.memory.total-allocated+"]
	require.NotNil(t, totalAlloc2)
	require.True(t, totalAlloc2.Value() > totalAlloc.Value())
}

func TestCumulativeCounter(t *testing.T) {
	scope := tally.NewTestScope("test", nil)
	counter := newCumulativeCounter(scope, "foo")

	last := counter.update(5)
	require.Equal(t, uint64(0), last)
	snapshot := scope.Snapshot()
	foo := snapshot.Counters()["test.foo+"]
	require.NotNil(t, foo)
	require.Equal(t, int64(5), foo.Value())

	last = counter.update(10)
	require.Equal(t, uint64(5), last)
	snapshot = scope.Snapshot()
	foo = snapshot.Counters()["test.foo+"]
	require.NotNil(t, foo)
	require.Equal(t, int64(10), foo.Value())
}
