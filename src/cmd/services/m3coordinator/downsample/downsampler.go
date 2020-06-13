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

	"github.com/m3db/m3/src/query/ts"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Downsampler is a downsampler.
type Downsampler interface {
	NewMetricsAppender() (MetricsAppender, error)
}

// MetricsAppender is a metrics appender that can build a samples
// appender, only valid to use with a single caller at a time.
type MetricsAppender interface {
	AddTag(name, value []byte)
	SamplesAppender(opts SampleAppenderOptions) (SamplesAppenderResult, error)
	Reset()
	Finalize()
}

// SamplesAppenderResult is the result from a SamplesAppender call.
type SamplesAppenderResult struct {
	SamplesAppender     SamplesAppender
	IsDropPolicyApplied bool
}

// SampleAppenderOptions defines the options being used when constructing
// the samples appender for a metric.
type SampleAppenderOptions struct {
	Override      bool
	OverrideRules SamplesAppenderOverrideRules
	MetricType    ts.MetricType
}

// SamplesAppenderOverrideRules provides override rules to
// use instead of matching against default and dynamic matched rules
// for an ID.
type SamplesAppenderOverrideRules struct {
	MappingRules []AutoMappingRule
}

// SamplesAppender is a downsampling samples appender,
// that can only be called by a single caller at a time.
type SamplesAppender interface {
	AppendCounterSample(value int64) error
	AppendGaugeSample(value float64) error
	AppendCounterTimedSample(t time.Time, value int64) error
	AppendGaugeTimedSample(t time.Time, value float64) error
	AppendTimerTimedSample(t time.Time, value float64) error
}

type downsampler struct {
	opts DownsamplerOptions
	agg  agg

	debugLogging bool
	logger       *zap.Logger
}

type downsamplerOptions struct {
	opts DownsamplerOptions
	agg  agg
}

func newDownsampler(opts downsamplerOptions) (*downsampler, error) {
	if err := opts.opts.validate(); err != nil {
		return nil, err
	}

	debugLogging := false
	logger := opts.opts.InstrumentOptions.Logger()
	if logger.Check(zapcore.DebugLevel, "debug") != nil {
		debugLogging = true
	}

	return &downsampler{
		opts:         opts.opts,
		agg:          opts.agg,
		debugLogging: debugLogging,
		logger:       logger,
	}, nil
}

func (d *downsampler) NewMetricsAppender() (MetricsAppender, error) {
	return newMetricsAppender(metricsAppenderOptions{
		agg:                    d.agg.aggregator,
		clientRemote:           d.agg.clientRemote,
		clockOpts:              d.agg.clockOpts,
		tagEncoder:             d.agg.pools.tagEncoderPool.Get(),
		matcher:                d.agg.matcher,
		metricTagsIteratorPool: d.agg.pools.metricTagsIteratorPool,
		debugLogging:           d.debugLogging,
		logger:                 d.logger,
	}), nil
}
