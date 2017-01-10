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

	"github.com/stretchr/testify/require"
)

func TestPolicyChange(t *testing.T) {
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
		numIDs   = 1
		start    = testSetup.getNowFn()
		middle   = start.Add(4 * time.Second)
		end      = start.Add(10 * time.Second)
		interval = time.Second
	)
	client := testSetup.newClient()
	require.NoError(t, client.connect())
	defer client.close()

	ids := generateTestIDs(idPrefix, numIDs)
	input1 := generateTestData(start, middle, interval, ids, roundRobinMetricTypeFn, testVersionedPolicies)
	input2 := generateTestData(middle, end, interval, ids, roundRobinMetricTypeFn, testUpdatedVersionedPolicies)
	for _, input := range []testDatasetWithPolicies{input1, input2} {
		for _, data := range input.dataset {
			testSetup.setNowFn(data.timestamp)
			for _, mu := range data.metrics {
				require.NoError(t, client.write(mu, input.policies))
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
	expected := toExpectedResults(finalTime, input1, testSetup.aggregatorOpts)
	expected = append(expected, toExpectedResults(finalTime, input2, testSetup.aggregatorOpts)...)
	actual := testSetup.sortedResults()
	require.Equal(t, expected, actual)
}
