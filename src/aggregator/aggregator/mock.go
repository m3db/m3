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
	"sync"

	"github.com/m3db/m3metrics/metric/unaggregated"
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

// NewMockAggregator creates a new mock aggregator
func NewMockAggregator() MockAggregator {
	return &mockAggregator{}
}

func (agg *mockAggregator) AddCounterWithPolicies(cp unaggregated.CounterWithPolicies) {
	agg.Lock()
	agg.countersWithPolicies = append(agg.countersWithPolicies, cp)
	agg.Unlock()
}

func (agg *mockAggregator) AddBatchTimerWithPolicies(btp unaggregated.BatchTimerWithPolicies) {
	agg.Lock()
	agg.batchTimersWithPolicies = append(agg.batchTimersWithPolicies, btp)
	agg.Unlock()
}

func (agg *mockAggregator) AddGaugeWithPolicies(gp unaggregated.GaugeWithPolicies) {
	agg.Lock()
	agg.gaugesWithPolicies = append(agg.gaugesWithPolicies, gp)
	agg.Unlock()
}

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
