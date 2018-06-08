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

package capture

import (
	"errors"
	"fmt"
	"sync"

	aggr "github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

// aggregator is an aggregator that simply captures metrics coming
// into the aggregator without actually performing aggregations.
// It is useful for testing purposes.
type aggregator struct {
	sync.RWMutex

	numMetricsAdded          int
	countersWithMetadatas    []unaggregated.CounterWithMetadatas
	batchTimersWithMetadatas []unaggregated.BatchTimerWithMetadatas
	gaugesWithMetadatas      []unaggregated.GaugeWithMetadatas
}

// NewAggregator creates a new capturing aggregator.
func NewAggregator() Aggregator {
	return &aggregator{}
}

func (agg *aggregator) Open() error { return nil }

func (agg *aggregator) AddUntimed(
	mu unaggregated.MetricUnion,
	sm metadata.StagedMetadatas,
) error {
	// Clone the metric and metadatas to ensure it cannot be mutated externally.
	mu = cloneMetric(mu)
	sm = cloneStagedMetadatas(sm)

	agg.Lock()
	defer agg.Unlock()

	switch mu.Type {
	case metric.CounterType:
		cp := unaggregated.CounterWithMetadatas{
			Counter:         mu.Counter(),
			StagedMetadatas: sm,
		}
		agg.countersWithMetadatas = append(agg.countersWithMetadatas, cp)
	case metric.TimerType:
		btp := unaggregated.BatchTimerWithMetadatas{
			BatchTimer:      mu.BatchTimer(),
			StagedMetadatas: sm,
		}
		agg.batchTimersWithMetadatas = append(agg.batchTimersWithMetadatas, btp)
	case metric.GaugeType:
		gp := unaggregated.GaugeWithMetadatas{
			Gauge:           mu.Gauge(),
			StagedMetadatas: sm,
		}
		agg.gaugesWithMetadatas = append(agg.gaugesWithMetadatas, gp)
	default:
		return fmt.Errorf("unrecognized metric type %v", mu.Type)
	}
	agg.numMetricsAdded++
	return nil
}

// TODO(xichen): implement this.
func (agg *aggregator) AddForwarded(
	metric aggregated.Metric,
	metadata metadata.ForwardMetadata,
) error {
	return errors.New("not implemented")
}

func (agg *aggregator) Resign() error              { return nil }
func (agg *aggregator) Status() aggr.RuntimeStatus { return aggr.RuntimeStatus{} }
func (agg *aggregator) Close() error               { return nil }

func (agg *aggregator) NumMetricsAdded() int {
	agg.RLock()
	numMetricsAdded := agg.numMetricsAdded
	agg.RUnlock()
	return numMetricsAdded
}

func (agg *aggregator) Snapshot() SnapshotResult {
	agg.Lock()

	result := SnapshotResult{
		CountersWithMetadatas:    agg.countersWithMetadatas,
		BatchTimersWithMetadatas: agg.batchTimersWithMetadatas,
		GaugesWithMetadatas:      agg.gaugesWithMetadatas,
	}
	agg.countersWithMetadatas = nil
	agg.batchTimersWithMetadatas = nil
	agg.gaugesWithMetadatas = nil
	agg.numMetricsAdded = 0

	agg.Unlock()

	return result
}

func cloneMetric(m unaggregated.MetricUnion) unaggregated.MetricUnion {
	mu := m

	// Clone metric ID.
	clonedID := make(id.RawID, len(m.ID))
	copy(clonedID, m.ID)
	mu.ID = clonedID

	// Clone time values.
	if m.Type == metric.TimerType {
		clonedTimerVal := make([]float64, len(m.BatchTimerVal))
		copy(clonedTimerVal, m.BatchTimerVal)
		mu.BatchTimerVal = clonedTimerVal
	}
	return mu
}

func cloneStagedMetadata(sm metadata.StagedMetadata) metadata.StagedMetadata {
	if sm.IsDefault() {
		return sm
	}
	pipelines := sm.Pipelines
	cloned := make([]metadata.PipelineMetadata, len(pipelines))
	for i := 0; i < len(pipelines); i++ {
		storagePolicies := make([]policy.StoragePolicy, len(pipelines[i].StoragePolicies))
		copy(storagePolicies, pipelines[i].StoragePolicies)
		pipeline := pipelines[i].Pipeline.Clone()
		cloned[i] = metadata.PipelineMetadata{
			AggregationID:   pipelines[i].AggregationID,
			StoragePolicies: storagePolicies,
			Pipeline:        pipeline,
		}
	}
	return metadata.StagedMetadata{
		Metadata:     metadata.Metadata{Pipelines: cloned},
		CutoverNanos: sm.CutoverNanos,
		Tombstoned:   sm.Tombstoned,
	}
}

func cloneStagedMetadatas(sm metadata.StagedMetadatas) metadata.StagedMetadatas {
	if sm.IsDefault() {
		return sm
	}
	cloned := make(metadata.StagedMetadatas, len(sm))
	for i := 0; i < len(sm); i++ {
		cloned[i] = cloneStagedMetadata(sm[i])
	}
	return cloned
}
