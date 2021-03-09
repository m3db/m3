// Copyright (c) 2018 Uber Technologies, Inc.
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

package downsample

import (
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// samplesAppender must have one of agg or client set
type samplesAppender struct {
	agg          aggregator.Aggregator
	clientRemote client.Client
	dropTs       bool

	unownedID       []byte
	stagedMetadatas metadata.StagedMetadatas
}

// Ensure samplesAppender implements SamplesAppender.
var _ SamplesAppender = (*samplesAppender)(nil)

func (a samplesAppender) AppendUntimedCounterSample(value int64, annotation []byte) error {
	if a.clientRemote != nil {
		// Remote client write instead of local aggregation.
		sample := unaggregated.Counter{
			ID:         a.unownedID,
			Value:      value,
			Annotation: annotation,
		}
		return a.clientRemote.WriteUntimedCounter(sample, a.stagedMetadatas)
	}

	sample := unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         a.unownedID,
		CounterVal: value,
		Annotation: annotation,
	}
	return a.agg.AddUntimed(sample, a.stagedMetadatas)
}

func (a samplesAppender) AppendUntimedGaugeSample(value float64, annotation []byte) error {
	if a.clientRemote != nil {
		// Remote client write instead of local aggregation.
		sample := unaggregated.Gauge{
			ID:         a.unownedID,
			Value:      value,
			Annotation: annotation,
		}
		return a.clientRemote.WriteUntimedGauge(sample, a.stagedMetadatas)
	}

	sample := unaggregated.MetricUnion{
		Type:       metric.GaugeType,
		ID:         a.unownedID,
		GaugeVal:   value,
		Annotation: annotation,
	}
	return a.agg.AddUntimed(sample, a.stagedMetadatas)
}

func (a samplesAppender) AppendUntimedTimerSample(value float64, annotation []byte) error {
	if a.clientRemote != nil {
		// Remote client write instead of local aggregation.
		sample := unaggregated.BatchTimer{
			ID:         a.unownedID,
			Values:     []float64{value},
			Annotation: annotation,
		}
		return a.clientRemote.WriteUntimedBatchTimer(sample, a.stagedMetadatas)
	}

	sample := unaggregated.MetricUnion{
		Type:          metric.TimerType,
		ID:            a.unownedID,
		BatchTimerVal: []float64{value},
		Annotation:    annotation,
	}
	return a.agg.AddUntimed(sample, a.stagedMetadatas)
}

func (a *samplesAppender) AppendCounterSample(t time.Time, value int64, annotation []byte) error {
	if a.dropTs {
		return a.AppendUntimedCounterSample(value, annotation)
	}
	return a.appendTimedSample(aggregated.Metric{
		Type:       metric.CounterType,
		ID:         a.unownedID,
		TimeNanos:  t.UnixNano(),
		Value:      float64(value),
		Annotation: annotation,
	})
}

func (a *samplesAppender) AppendGaugeSample(t time.Time, value float64, annotation []byte) error {
	if a.dropTs {
		return a.AppendUntimedGaugeSample(value, annotation)
	}
	return a.appendTimedSample(aggregated.Metric{
		Type:       metric.GaugeType,
		ID:         a.unownedID,
		TimeNanos:  t.UnixNano(),
		Value:      value,
		Annotation: annotation,
	})
}

func (a *samplesAppender) AppendTimerSample(t time.Time, value float64, annotation []byte) error {
	if a.dropTs {
		return a.AppendUntimedTimerSample(value, annotation)
	}
	return a.appendTimedSample(aggregated.Metric{
		Type:       metric.TimerType,
		ID:         a.unownedID,
		TimeNanos:  t.UnixNano(),
		Value:      value,
		Annotation: annotation,
	})
}

func (a *samplesAppender) appendTimedSample(sample aggregated.Metric) error {
	if a.clientRemote != nil {
		return a.clientRemote.WriteTimedWithStagedMetadatas(sample, a.stagedMetadatas)
	}

	return a.agg.AddTimedWithStagedMetadatas(sample, a.stagedMetadatas)
}

// Ensure multiSamplesAppender implements SamplesAppender.
var _ SamplesAppender = (*multiSamplesAppender)(nil)

type multiSamplesAppender struct {
	appenders []samplesAppender
}

func newMultiSamplesAppender() *multiSamplesAppender {
	return &multiSamplesAppender{}
}

func (a *multiSamplesAppender) reset() {
	for i := range a.appenders {
		a.appenders[i] = samplesAppender{}
	}
	a.appenders = a.appenders[:0]
}

func (a *multiSamplesAppender) addSamplesAppender(v samplesAppender) {
	a.appenders = append(a.appenders, v)
}

func (a *multiSamplesAppender) AppendUntimedCounterSample(value int64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendUntimedCounterSample(value, annotation))
	}
	return multiErr.LastError()
}

func (a *multiSamplesAppender) AppendUntimedGaugeSample(value float64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendUntimedGaugeSample(value, annotation))
	}
	return multiErr.LastError()
}

func (a *multiSamplesAppender) AppendUntimedTimerSample(value float64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendUntimedTimerSample(value, annotation))
	}
	return multiErr.LastError()
}

func (a *multiSamplesAppender) AppendCounterSample(t time.Time, value int64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendCounterSample(t, value, annotation))
	}
	return multiErr.LastError()
}

func (a *multiSamplesAppender) AppendGaugeSample(t time.Time, value float64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendGaugeSample(t, value, annotation))
	}
	return multiErr.LastError()
}

func (a *multiSamplesAppender) AppendTimerSample(t time.Time, value float64, annotation []byte) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendTimerSample(t, value, annotation))
	}
	return multiErr.LastError()
}
