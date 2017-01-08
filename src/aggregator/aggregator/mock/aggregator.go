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

package mock

import (
	"fmt"
	"sync"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

// mockAggregator is a no-op aggregator that simply captures
// metrics coming into the aggregator without actually performing
// aggregations
type mockAggregator struct {
	sync.RWMutex

	countersWithPolicies    []unaggregated.CounterWithPolicies
	batchTimersWithPolicies []unaggregated.BatchTimerWithPolicies
	gaugesWithPolicies      []unaggregated.GaugeWithPolicies
}

// NewAggregator creates a new mock aggregator
func NewAggregator() Aggregator {
	return &mockAggregator{}
}

func (agg *mockAggregator) AddMetricWithPolicies(
	mu unaggregated.MetricUnion,
	policies policy.VersionedPolicies,
) error {
	agg.Lock()

	switch mu.Type {
	case unaggregated.CounterType:
		cp := unaggregated.CounterWithPolicies{
			Counter:           mu.Counter(),
			VersionedPolicies: policies,
		}
		agg.countersWithPolicies = append(agg.countersWithPolicies, cp)
	case unaggregated.BatchTimerType:
		btp := unaggregated.BatchTimerWithPolicies{
			BatchTimer:        mu.BatchTimer(),
			VersionedPolicies: policies,
		}
		agg.batchTimersWithPolicies = append(agg.batchTimersWithPolicies, btp)
	case unaggregated.GaugeType:
		gp := unaggregated.GaugeWithPolicies{
			Gauge:             mu.Gauge(),
			VersionedPolicies: policies,
		}
		agg.gaugesWithPolicies = append(agg.gaugesWithPolicies, gp)
	default:
		agg.Unlock()
		return fmt.Errorf("unrecognized metric type %v", mu.Type)
	}

	agg.Unlock()

	return nil
}

func (agg *mockAggregator) Close() {}

func (agg *mockAggregator) Snapshot() SnapshotResult {
	agg.Lock()

	result := SnapshotResult{
		CountersWithPolicies:    agg.countersWithPolicies,
		BatchTimersWithPolicies: agg.batchTimersWithPolicies,
		GaugesWithPolicies:      agg.gaugesWithPolicies,
	}
	agg.countersWithPolicies = nil
	agg.batchTimersWithPolicies = nil
	agg.gaugesWithPolicies = nil

	agg.Unlock()

	return result
}
