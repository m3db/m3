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

package server

import (
	"testing"

	"github.com/m3db/m3aggregator/aggregator/mock"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestPacketProcessor(t *testing.T) {
	clockOpts := clock.NewOptions()
	instrumentOpts := instrument.NewOptions()
	queue := newPacketQueue(1024, clockOpts, instrumentOpts)
	aggregator := mock.NewAggregator()
	processor := newPacketProcessor(queue, aggregator, 1, clockOpts, instrumentOpts)

	// Processing invalid packets should result in an error
	require.Error(t, processor.processPacket(packet{}))

	// Add a few metrics
	var (
		expectedResult mock.SnapshotResult
		numIter        = 10
	)
	for i := 0; i < numIter; i++ {
		// Add test metrics to expected result
		expectedResult.CountersWithPolicies = append(expectedResult.CountersWithPolicies, testCounterWithPolicies)
		expectedResult.BatchTimersWithPolicies = append(expectedResult.BatchTimersWithPolicies, testBatchTimerWithPolicies)
		expectedResult.GaugesWithPolicies = append(expectedResult.GaugesWithPolicies, testGaugeWithPolicies)

		require.NoError(t, queue.Enqueue(packet{
			metric:   testCounter,
			policies: testCounterWithPolicies.VersionedPolicies,
		}))
		require.NoError(t, queue.Enqueue(packet{
			metric:   testBatchTimer,
			policies: testBatchTimerWithPolicies.VersionedPolicies,
		}))
		require.NoError(t, queue.Enqueue(packet{
			metric:   testGauge,
			policies: testGaugeWithPolicies.VersionedPolicies,
		}))
	}

	// Close the queue so the workers will finish
	queue.Close()

	// Close the processor
	processor.Close()

	// Closing the processor a second time should be a no-op
	processor.Close()

	// Assert the snapshot match expectations
	require.Equal(t, expectedResult, processor.aggregator.(mock.Aggregator).Snapshot())
}
