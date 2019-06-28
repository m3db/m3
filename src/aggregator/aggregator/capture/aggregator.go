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
	"fmt"
	"sync"

	aggr "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/policy"
)

// aggregator is an aggregator that simply captures metrics coming
// into the aggregator without actually performing aggregations.
// It is useful for testing purposes.
type aggregator struct {
	sync.RWMutex

	numMetricsAdded                int
	countersWithMetadatas          []unaggregated.CounterWithMetadatas
	batchTimersWithMetadatas       []unaggregated.BatchTimerWithMetadatas
	gaugesWithMetadatas            []unaggregated.GaugeWithMetadatas
	forwardedMetricsWithMetadata   []aggregated.ForwardedMetricWithMetadata
	timedMetricsWithMetadata       []aggregated.TimedMetricWithMetadata
	passThroughMetricsWithMetadata []aggregated.TimedMetricWithMetadata
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
	mu = cloneUntimedMetric(mu)
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

func (agg *aggregator) AddTimed(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	// Clone the metric and timed metadata to ensure it cannot be mutated externally.
	metric = cloneTimedMetric(metric)
	metadata = cloneTimedMetadata(metadata)

	agg.Lock()
	defer agg.Unlock()

	tm := aggregated.TimedMetricWithMetadata{
		Metric:        metric,
		TimedMetadata: metadata,
	}
	agg.timedMetricsWithMetadata = append(agg.timedMetricsWithMetadata, tm)
	agg.numMetricsAdded++
	return nil
}

func (agg *aggregator) AddForwarded(
	metric aggregated.ForwardedMetric,
	metadata metadata.ForwardMetadata,
) error {
	// Clone the metric and forward metadata to ensure it cannot be mutated externally.
	metric = cloneForwardedMetric(metric)
	metadata = cloneForwardMetadata(metadata)

	agg.Lock()
	defer agg.Unlock()

	mf := aggregated.ForwardedMetricWithMetadata{
		ForwardedMetric: metric,
		ForwardMetadata: metadata,
	}
	agg.forwardedMetricsWithMetadata = append(agg.forwardedMetricsWithMetadata, mf)
	agg.numMetricsAdded++
	return nil
}

func (agg *aggregator) AddPassThrough(
	metric aggregated.Metric,
	metadata metadata.TimedMetadata,
) error {
	// Clone the metric and timed metadata to ensure it cannot be mutated externally.
	metric = cloneTimedMetric(metric)
	metadata = cloneTimedMetadata(metadata)

	agg.Lock()
	defer agg.Unlock()

	tm := aggregated.TimedMetricWithMetadata{
		Metric:        metric,
		TimedMetadata: metadata,
	}
	agg.passThroughMetricsWithMetadata = append(agg.passThroughMetricsWithMetadata, tm)
	agg.numMetricsAdded++
	return nil
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
		CountersWithMetadatas:         agg.countersWithMetadatas,
		BatchTimersWithMetadatas:      agg.batchTimersWithMetadatas,
		GaugesWithMetadatas:           agg.gaugesWithMetadatas,
		ForwardedMetricsWithMetadata:  agg.forwardedMetricsWithMetadata,
		TimedMetricWithMetadata:       agg.timedMetricsWithMetadata,
		PassThroughMetricWithMetadata: agg.passThroughMetricsWithMetadata,
	}
	agg.countersWithMetadatas = nil
	agg.batchTimersWithMetadatas = nil
	agg.gaugesWithMetadatas = nil
	agg.forwardedMetricsWithMetadata = nil
	agg.timedMetricsWithMetadata = nil
	agg.passThroughMetricsWithMetadata = nil
	agg.numMetricsAdded = 0

	agg.Unlock()

	return result
}

func cloneUntimedMetric(m unaggregated.MetricUnion) unaggregated.MetricUnion {
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

func cloneForwardedMetric(metric aggregated.ForwardedMetric) aggregated.ForwardedMetric {
	cloned := metric
	cloned.ID = make(id.RawID, len(metric.ID))
	copy(cloned.ID, metric.ID)
	cloned.Values = make([]float64, len(metric.Values))
	copy(cloned.Values, metric.Values)
	return cloned
}

func cloneForwardMetadata(meta metadata.ForwardMetadata) metadata.ForwardMetadata {
	cloned := meta
	cloned.Pipeline = meta.Pipeline.Clone()
	return cloned
}

func cloneTimedMetric(metric aggregated.Metric) aggregated.Metric {
	cloned := metric
	cloned.ID = make(id.RawID, len(metric.ID))
	copy(cloned.ID, metric.ID)
	cloned.Value = metric.Value
	return cloned
}

func cloneTimedMetadata(meta metadata.TimedMetadata) metadata.TimedMetadata {
	cloned := meta
	return cloned
}
