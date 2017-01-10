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

package packet

import (
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator/mock"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testCounter = unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         []byte("foo"),
		CounterVal: 123,
	}
	testBatchTimer = unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            []byte("bar"),
		BatchTimerVal: []float64{1.0, 2.0, 3.0},
	}
	testGauge = unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       []byte("baz"),
		GaugeVal: 456.780,
	}
	testCounterWithPolicies = unaggregated.CounterWithPolicies{
		Counter:           testCounter.Counter(),
		VersionedPolicies: policy.DefaultVersionedPolicies,
	}
	testBatchTimerWithPolicies = unaggregated.BatchTimerWithPolicies{
		BatchTimer: testBatchTimer.BatchTimer(),
		VersionedPolicies: policy.VersionedPolicies{
			Version: 1,
			Cutover: time.Now(),
			Policies: []policy.Policy{
				{
					Resolution: policy.Resolution{Window: time.Duration(1), Precision: xtime.Second},
					Retention:  policy.Retention(time.Hour),
				},
			},
		},
	}
	testGaugeWithPolicies = unaggregated.GaugeWithPolicies{
		Gauge:             testGauge.Gauge(),
		VersionedPolicies: policy.DefaultVersionedPolicies,
	}
)

func TestProcessor(t *testing.T) {
	clockOpts := clock.NewOptions()
	instrumentOpts := instrument.NewOptions()
	queue := NewQueue(1024, clockOpts, instrumentOpts)
	aggregator := mock.NewAggregator()
	processor := NewProcessor(queue, aggregator, 1, clockOpts, instrumentOpts)

	// Processing invalid packets should result in an error
	require.Error(t, processor.processPacket(Packet{}))

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

		require.NoError(t, queue.Enqueue(Packet{
			Metric:   testCounter,
			Policies: testCounterWithPolicies.VersionedPolicies,
		}))
		require.NoError(t, queue.Enqueue(Packet{
			Metric:   testBatchTimer,
			Policies: testBatchTimerWithPolicies.VersionedPolicies,
		}))
		require.NoError(t, queue.Enqueue(Packet{
			Metric:   testGauge,
			Policies: testGaugeWithPolicies.VersionedPolicies,
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
