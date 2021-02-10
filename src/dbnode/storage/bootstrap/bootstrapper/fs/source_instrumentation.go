// Copyright (c) 2021 Uber Technologies, Inc.
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

package fs

import (
	"time"

	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

type instrumentationContext struct {
	opts                   Options
	nowFn                  clock.NowFn
	log                    *zap.Logger
	start                  time.Time
	span                   opentracing.Span
	bootstrapDataDuration  tally.Timer
	bootstrapIndexDuration tally.Timer
	profiler               instrument.Profiler
	logFields              []zapcore.Field
}

func newInstrumentationContext(
	opts Options,
	nowFn clock.NowFn,
	log *zap.Logger,
	span opentracing.Span,
	scope tally.Scope,
	profiler instrument.Profiler,
) *instrumentationContext {
	return &instrumentationContext{
		opts:                   opts,
		nowFn:                  nowFn,
		log:                    log,
		span:                   span,
		profiler:               profiler,
		bootstrapDataDuration:  scope.Timer("data-duration"),
		bootstrapIndexDuration: scope.Timer("index-duration"),
		logFields: []zapcore.Field{
			zap.Stringer("cachePolicy", opts.ResultOptions().SeriesCachePolicy()),
		},
	}
}

const (
	dataProfile  = "fs-data"
	indexProfile = "fs-index"
)

func (i *instrumentationContext) finish() {
	i.span.Finish()
}

func (i *instrumentationContext) startCPUProfile(name string) {
	err := i.profiler.StartCPUProfile(name)
	if err != nil {
		i.log.Error("unable to start cpu profile", zap.Error(err))
	}
}

func (i *instrumentationContext) stopCPUProfile() {
	if err := i.profiler.StopCPUProfile(); err != nil {
		i.log.Error("unable to stop cpu profile", zap.Error(err))
	}
}

func (i *instrumentationContext) writeHeapProfile(name string) {
	err := i.profiler.WriteHeapProfile(name)
	if err != nil {
		i.log.Error("unable to write heap profile", zap.Error(err))
	}
}

func (i *instrumentationContext) bootstrapDataStarted() {
	i.log.Info("bootstrapping time series data start", i.logFields...)
	i.span.LogFields(opentracinglog.String("event", "bootstrap_data_start"))
	i.start = i.nowFn()
	i.startCPUProfile(dataProfile)
	i.writeHeapProfile(dataProfile)
}

func (i *instrumentationContext) bootstrapDataCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapDataDuration.Record(duration)
	i.log.Info("bootstrapping time series data success",
		append(i.logFields, zap.Duration("took", duration))...)
	i.span.LogFields(opentracinglog.String("event", "bootstrap_data_done"))
	i.stopCPUProfile()
	i.writeHeapProfile(dataProfile)
}

func (i *instrumentationContext) bootstrapIndexStarted() {
	i.log.Info("bootstrapping index metadata start")
	i.span.LogFields(opentracinglog.String("event", "bootstrap_index_start"))
	i.start = i.nowFn()
	i.startCPUProfile(indexProfile)
	i.writeHeapProfile(indexProfile)
}

func (i *instrumentationContext) bootstrapIndexCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapIndexDuration.Record(duration)
	i.log.Info("bootstrapping index metadata success", zap.Duration("took", duration))
	i.span.LogFields(opentracinglog.String("event", "bootstrap_index_done"))
	i.stopCPUProfile()
	i.writeHeapProfile(indexProfile)
}

type instrumentation struct {
	opts     Options
	profiler instrument.Profiler
	scope    tally.Scope
	log      *zap.Logger
	nowFn    clock.NowFn
}

func newInstrumentation(opts Options, scope tally.Scope, iOpts instrument.Options) *instrumentation {
	return &instrumentation{
		opts:     opts,
		profiler: iOpts.Profiler(),
		scope:    scope,
		log:      iOpts.Logger().With(zap.String("bootstrapper", "filesystem")),
		nowFn:    opts.ResultOptions().ClockOptions().NowFn(),
	}
}

func (i *instrumentation) fsBootstrapperSourceReadStarted(
	ctx context.Context,
) *instrumentationContext {
	_, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperFilesystemSourceRead)
	return newInstrumentationContext(
		i.opts,
		i.nowFn,
		i.log,
		span,
		i.scope,
		i.profiler,
	)
}
