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

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

// mockAggregator is a no-op aggregator that simply captures
// metrics coming into the aggregator without actually performing
// aggregations
type mockAggregator struct {
	sync.RWMutex

	numMetricsAdded             int
	countersWithPoliciesList    []unaggregated.CounterWithPoliciesList
	batchTimersWithPoliciesList []unaggregated.BatchTimerWithPoliciesList
	gaugesWithPoliciesList      []unaggregated.GaugeWithPoliciesList
}

// NewAggregator creates a new mock aggregator
func NewAggregator() Aggregator {
	return &mockAggregator{}
}

func (agg *mockAggregator) Open() error { return nil }

func (agg *mockAggregator) AddMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	// Clone the metric and policies to ensure it cannot be mutated externally.
	mu = cloneMetric(mu)
	pl = clonePoliciesList(pl)

	agg.Lock()

	switch mu.Type {
	case unaggregated.CounterType:
		cp := unaggregated.CounterWithPoliciesList{
			Counter:      mu.Counter(),
			PoliciesList: pl,
		}
		agg.countersWithPoliciesList = append(agg.countersWithPoliciesList, cp)
	case unaggregated.BatchTimerType:
		btp := unaggregated.BatchTimerWithPoliciesList{
			BatchTimer:   mu.BatchTimer(),
			PoliciesList: pl,
		}
		agg.batchTimersWithPoliciesList = append(agg.batchTimersWithPoliciesList, btp)
	case unaggregated.GaugeType:
		gp := unaggregated.GaugeWithPoliciesList{
			Gauge:        mu.Gauge(),
			PoliciesList: pl,
		}
		agg.gaugesWithPoliciesList = append(agg.gaugesWithPoliciesList, gp)
	default:
		agg.Unlock()
		return fmt.Errorf("unrecognized metric type %v", mu.Type)
	}

	agg.numMetricsAdded++
	agg.Unlock()

	return nil
}

func (agg *mockAggregator) Resign() error                    { return nil }
func (agg *mockAggregator) Status() aggregator.RuntimeStatus { return aggregator.RuntimeStatus{} }
func (agg *mockAggregator) Close() error                     { return nil }

func (agg *mockAggregator) NumMetricsAdded() int {
	agg.RLock()
	numMetricsAdded := agg.numMetricsAdded
	agg.RUnlock()
	return numMetricsAdded
}

func (agg *mockAggregator) Snapshot() SnapshotResult {
	agg.Lock()

	result := SnapshotResult{
		CountersWithPoliciesList:    agg.countersWithPoliciesList,
		BatchTimersWithPoliciesList: agg.batchTimersWithPoliciesList,
		GaugesWithPoliciesList:      agg.gaugesWithPoliciesList,
	}
	agg.countersWithPoliciesList = nil
	agg.batchTimersWithPoliciesList = nil
	agg.gaugesWithPoliciesList = nil
	agg.numMetricsAdded = 0

	agg.Unlock()

	return result
}

func cloneMetric(m unaggregated.MetricUnion) unaggregated.MetricUnion {
	mu := m
	if !m.OwnsID {
		clonedID := make(id.RawID, len(m.ID))
		copy(clonedID, m.ID)
		mu.ID = clonedID
		mu.OwnsID = true
	}
	if m.Type == unaggregated.BatchTimerType {
		clonedTimerVal := make([]float64, len(m.BatchTimerVal))
		copy(clonedTimerVal, m.BatchTimerVal)
		mu.BatchTimerVal = clonedTimerVal
	}
	return mu
}

func cloneStagedPolicies(sp policy.StagedPolicies) policy.StagedPolicies {
	if sp.IsDefault() {
		return sp
	}
	policies, _ := sp.Policies()
	cloned := make([]policy.Policy, len(policies))
	for i, policy := range policies {
		cloned[i] = policy
	}
	return policy.NewStagedPolicies(sp.CutoverNanos, sp.Tombstoned, cloned)
}

func clonePoliciesList(pl policy.PoliciesList) policy.PoliciesList {
	if pl.IsDefault() {
		return pl
	}
	cloned := make(policy.PoliciesList, len(pl))
	for i := 0; i < len(pl); i++ {
		cloned[i] = cloneStagedPolicies(pl[i])
	}
	return cloned
}
