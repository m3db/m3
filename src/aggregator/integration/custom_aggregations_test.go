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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3metrics/metric/aggregated"

	"github.com/stretchr/testify/require"
)

func TestCustomAggregationWithPoliciesList(t *testing.T) {
	metadatas := [4]metadataUnion{
		{
			mType:        policiesListType,
			policiesList: testPoliciesList,
		},
		{
			mType:        policiesListType,
			policiesList: testPoliciesListWithCustomAggregation1,
		},
		{
			mType:        policiesListType,
			policiesList: testPoliciesListWithCustomAggregation2,
		},
		{
			mType:        policiesListType,
			policiesList: testPoliciesList,
		},
	}
	testCustomAggregations(t, metadatas)
}

func TestCustomAggregationWithStagedMetadatas(t *testing.T) {
	metadatas := [4]metadataUnion{
		{
			mType:           stagedMetadatasType,
			stagedMetadatas: testStagedMetadatas,
		},
		{
			mType:           stagedMetadatasType,
			stagedMetadatas: testUpdatedStagedMetadatas,
		},
		{
			mType:           stagedMetadatasType,
			stagedMetadatas: testStagedMetadatas,
		},
		{
			mType:           stagedMetadatasType,
			stagedMetadatas: testUpdatedStagedMetadatas,
		},
	}
	testCustomAggregations(t, metadatas)
}

func testCustomAggregations(t *testing.T, metadatas [4]metadataUnion) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setup.
	testSetup := newTestSetup(t, newTestOptions())
	defer testSetup.close()

	testSetup.aggregatorOpts =
		testSetup.aggregatorOpts.
			SetEntryCheckInterval(time.Second).
			SetMinFlushInterval(0)

	// Start the server.
	log := testSetup.aggregatorOpts.InstrumentOptions().Logger()
	log.Info("test custom aggregations")
	require.NoError(t, testSetup.startServer())
	log.Info("server is now up")
	require.NoError(t, testSetup.waitUntilLeader())
	log.Info("server is now the leader")

	var (
		idPrefix = "foo"
		numIDs   = 100
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
	inputs := []struct {
		dataset  testDataset
		metadata metadataUnion
	}{
		{
			dataset:  generateTestDataset(start, t1, interval, ids, roundRobinMetricTypeFn),
			metadata: metadatas[0],
		},
		{
			dataset:  generateTestDataset(t1, t2, interval, ids, roundRobinMetricTypeFn),
			metadata: metadatas[1],
		},
		{
			dataset:  generateTestDataset(t2, t3, interval, ids, roundRobinMetricTypeFn),
			metadata: metadatas[2],
		},
		{
			dataset:  generateTestDataset(t3, end, interval, ids, roundRobinMetricTypeFn),
			metadata: metadatas[3],
		},
	}
	for _, input := range inputs {
		for _, data := range input.dataset {
			testSetup.setNowFn(data.timestamp)
			for _, mu := range data.metrics {
				if input.metadata.mType == policiesListType {
					require.NoError(t, client.writeMetricWithPoliciesList(mu, input.metadata.policiesList))
				} else {
					require.NoError(t, client.writeMetricWithMetadatas(mu, input.metadata.stagedMetadatas))
				}
			}
			require.NoError(t, client.flush())

			// Give server some time to process the incoming packets.
			time.Sleep(100 * time.Millisecond)
		}
	}

	// Move time forward and wait for ticking to happen. The sleep time
	// must be the longer than the lowest resolution across all policies.
	finalTime := end.Add(time.Second)
	testSetup.setNowFn(finalTime)
	time.Sleep(6 * time.Second)

	// Stop the server.
	require.NoError(t, testSetup.stopServer())
	log.Info("server is now down")

	// Validate results.
	var expected []aggregated.MetricWithStoragePolicy
	for _, input := range inputs {
		expected = append(expected, computeExpectedResults(t, finalTime, input.dataset, input.metadata, testSetup.aggregatorOpts)...)
	}
	sort.Sort(byTimeIDPolicyAscending(expected))
	actual := testSetup.sortedResults()
	require.Equal(t, expected, actual)
}
