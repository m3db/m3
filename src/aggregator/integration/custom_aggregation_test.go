// +build integration

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

package integration

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	compressor                             = policy.NewAggregationIDCompressor()
	compressedLower, _                     = compressor.Compress(policy.AggregationTypes{policy.Lower})
	compressedLowerAndUpper, _             = compressor.Compress(policy.AggregationTypes{policy.Lower, policy.Upper})
	testPoliciesListWithCustomAggregation1 = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), compressedLower),
				policy.NewPolicy(policy.NewStoragePolicy(2*time.Second, xtime.Second, 6*time.Hour), compressedLower),
			},
		),
	}

	testPoliciesListWithCustomAggregation2 = policy.PoliciesList{
		policy.NewStagedPolicies(
			0,
			false,
			[]policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour), compressedLowerAndUpper),
				policy.NewPolicy(policy.NewStoragePolicy(3*time.Second, xtime.Second, 24*time.Hour), compressedLowerAndUpper),
			},
		),
	}
)

func TestCustomAggregation(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	testSetup.aggregatorOpts =
		testSetup.aggregatorOpts.
			SetEntryCheckInterval(time.Second).
			SetMinFlushInterval(0)

	// Start the server
	log := testSetup.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test policy change")
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")

	var (
		idPrefix = "foo"
		numIDs   = 3
		start    = testSetup.getNowFn()
		t1       = start.Add(2 * time.Second)
		t2       = start.Add(4 * time.Second)
		t3       = start.Add(6 * time.Second)
		end      = start.Add(8 * time.Second)
		interval = time.Second
	)
	client := testSetup.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	ids := generateTestIDs(idPrefix, numIDs)
	input1 := generateTestData(t, start, t1, interval, ids, roundRobinMetricTypeFn, testPoliciesList)
	input2 := generateTestData(t, t1, t2, interval, ids, roundRobinMetricTypeFn, testPoliciesListWithCustomAggregation1)
	input3 := generateTestData(t, t2, t3, interval, ids, roundRobinMetricTypeFn, testPoliciesListWithCustomAggregation2)
	input4 := generateTestData(t, t3, end, interval, ids, roundRobinMetricTypeFn, testPoliciesList)
	for _, input := range []testDatasetWithPoliciesList{input1, input2, input3, input4} {
		for _, data := range input.dataset {
			testSetup.setNowFn(data.timestamp)
			for _, mu := range data.metrics {
				require.NoError(t, client.write(mu, input.policiesList))
			}
			require.NoError(t, client.flush())

			// Give server some time to process the incoming packets
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := end.Add(time.Second)
	testSetup.setNowFn(finalTime)
	time.Sleep(6 * time.Second)

	// Stop the server
	require.NoError(t, testSetup.stopServer())
	log.Info("server is now down")

	// Validate results
	expected := toExpectedResults(t, finalTime, input1, testSetup.aggregatorOpts)
	expected = append(expected, toExpectedResults(t, finalTime, input2, testSetup.aggregatorOpts)...)
	expected = append(expected, toExpectedResults(t, finalTime, input3, testSetup.aggregatorOpts)...)
	expected = append(expected, toExpectedResults(t, finalTime, input4, testSetup.aggregatorOpts)...)
	actual := testSetup.sortedResults()
	require.Equal(t, expected, actual)
}
