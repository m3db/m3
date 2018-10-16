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
	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	xerrors "github.com/m3db/m3x/errors"
)

type samplesAppender struct {
	agg             aggregator.Aggregator
	unownedID       []byte
	stagedMetadatas metadata.StagedMetadatas
}

func (a samplesAppender) AppendCounterSample(value int64) error {
	sample := unaggregated.MetricUnion{
		Type:       metric.CounterType,
		ID:         a.unownedID,
		CounterVal: value,
	}
	return a.agg.AddUntimed(sample, a.stagedMetadatas)
}

func (a samplesAppender) AppendGaugeSample(value float64) error {
	sample := unaggregated.MetricUnion{
		Type:     metric.GaugeType,
		ID:       a.unownedID,
		GaugeVal: value,
	}
	return a.agg.AddUntimed(sample, a.stagedMetadatas)
}

// Ensure multiSamplesAppender implements SamplesAppender
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

func (a *multiSamplesAppender) AppendCounterSample(value int64) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendCounterSample(value))
	}
	return multiErr.FinalError()
}

func (a *multiSamplesAppender) AppendGaugeSample(value float64) error {
	var multiErr xerrors.MultiError
	for _, appender := range a.appenders {
		multiErr = multiErr.Add(appender.AppendGaugeSample(value))
	}
	return multiErr.FinalError()
}
