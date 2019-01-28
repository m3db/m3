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

package instrument

import (
	"github.com/opentracing/opentracing-go"
	"time"

	"github.com/m3db/m3x/log"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	defaultSamplingRate      = 1.0
	defaultReportingInterval = time.Second
)

type options struct {
	logger         log.Logger
	zap            *zap.Logger
	scope          tally.Scope
	tracer         opentracing.Tracer
	samplingRate   float64
	reportInterval time.Duration
}

// NewOptions creates new instrument options.
func NewOptions() Options {
	logger := log.NewLevelLogger(log.SimpleLogger, log.LevelInfo)
	return &options{
		logger:         logger,
		zap:            zap.L(),
		scope:          tally.NoopScope,
		samplingRate:   defaultSamplingRate,
		reportInterval: defaultReportingInterval,
	}
}

func (o *options) SetLogger(value log.Logger) Options {
	opts := *o
	opts.logger = value
	return &opts
}

func (o *options) Logger() log.Logger {
	return o.logger
}

func (o *options) SetZapLogger(value *zap.Logger) Options {
	opts := *o
	opts.zap = value
	return &opts
}

func (o *options) ZapLogger() *zap.Logger {
	return o.zap
}

func (o *options) SetMetricsScope(value tally.Scope) Options {
	opts := *o
	opts.scope = value
	return &opts
}

func (o *options) MetricsScope() tally.Scope {
	return o.scope
}

func (o *options) Tracer() opentracing.Tracer {
	return o.tracer
}

func (o *options) SetTracer(tracer opentracing.Tracer) Options {
	opts := *o
	opts.tracer = tracer
	return &opts
}

func (o *options) SetMetricsSamplingRate(value float64) Options {
	opts := *o
	opts.samplingRate = value
	return &opts
}

func (o *options) MetricsSamplingRate() float64 {
	return o.samplingRate
}

func (o *options) SetReportInterval(value time.Duration) Options {
	opts := *o
	opts.reportInterval = value
	return &opts
}

func (o *options) ReportInterval() time.Duration {
	return o.reportInterval
}
