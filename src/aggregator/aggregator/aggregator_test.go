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

package aggregator

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func TestAggregator(t *testing.T) {
	agg := NewAggregator(testOptions().SetEntryCheckInterval(0)).(*aggregator)

	var (
		resultMu       unaggregated.MetricUnion
		resultPolicies policy.VersionedPolicies
	)
	agg.addMetricWithPoliciesFn = func(
		mu unaggregated.MetricUnion,
		policies policy.VersionedPolicies,
	) error {
		resultMu = mu
		resultPolicies = policies
		return nil
	}
	agg.waitForFn = func(time.Duration) <-chan time.Time {
		c := make(chan time.Time)
		close(c)
		return c
	}

	// Add a counter metric
	require.NoError(t, agg.AddMetricWithPolicies(testCounter, testCustomVersionedPolicies))
	require.Equal(t, testCounter, resultMu)
	require.Equal(t, testCustomVersionedPolicies, resultPolicies)

	// Force a tick
	agg.tickInternal()

	// Close the aggregator
	agg.Close()

	// Closing a second time should be a no op
	agg.Close()

	// Adding a metric to a closed aggregator should result in an error
	err := agg.AddMetricWithPolicies(testCounter, testCustomVersionedPolicies)
	require.Equal(t, errAggregatorClosed, err)

	// Assert the aggregator is closed
	require.Equal(t, int32(1), atomic.LoadInt32(&agg.closed))
}
