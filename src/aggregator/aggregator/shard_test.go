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

package aggregator

import (
	"testing"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/stretchr/testify/require"
)

func TestAggregatorShard(t *testing.T) {
	shard := newAggregatorShard(0, testOptions().SetEntryCheckInterval(0))
	require.Equal(t, uint32(0), shard.ID())

	var (
		resultMu           unaggregated.MetricUnion
		resultPoliciesList policy.PoliciesList
	)
	shard.addMetricWithPoliciesListFn = func(
		mu unaggregated.MetricUnion,
		pl policy.PoliciesList,
	) error {
		resultMu = mu
		resultPoliciesList = pl
		return nil
	}

	require.NoError(t, shard.AddMetricWithPoliciesList(testValidMetric, testPoliciesList))
	require.Equal(t, testValidMetric, resultMu)
	require.Equal(t, testPoliciesList, resultPoliciesList)

	// Close the shard.
	shard.Close()

	// Adding a metric to a closed shard should result in an error.
	err := shard.AddMetricWithPoliciesList(testValidMetric, testPoliciesList)
	require.Equal(t, errAggregatorShardClosed, err)

	// Assert the shard is closed.
	require.True(t, shard.closed)
}
