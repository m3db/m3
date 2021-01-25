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

package uninitialized

import (
	"time"

	"github.com/opentracing/opentracing-go"
	opentracinglog "github.com/opentracing/opentracing-go/log"
	"github.com/uber-go/tally"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/storage/profiler"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
)

type instrumentationContext struct {
	nowFn                 clock.NowFn
	log                   *zap.Logger
	start                 time.Time
	span                  opentracing.Span
	bootstrapReadDuration tally.Timer
	pOpts                 profiler.Options
	pCtx                  profiler.ProfileContext
}

func newInstrumentationContext(
	nowFn clock.NowFn,
	log *zap.Logger,
	span opentracing.Span,
	scope tally.Scope,
	pOpts profiler.Options,
) *instrumentationContext {
	ctx := &instrumentationContext{
		nowFn:                 nowFn,
		log:                   log,
		span:                  span,
		pOpts:                 pOpts,
		bootstrapReadDuration: scope.Timer("read-duration"),
	}
	ctx.readStarted()
	return ctx
}

func (i *instrumentationContext) finish() {
	i.readCompleted()
	i.span.Finish()
}

func (i *instrumentationContext) startProfileIfEnabled(name string) {
	if i.pOpts.Enabled() {
		profileContext, err := i.pOpts.Profiler().StartProfile(name)
		if err != nil {
			i.log.Error("unable to start profile", zap.Error(err))
		}
		i.pCtx = profileContext
	}
}

func (i *instrumentationContext) stopProfileIfEnabled() {
	if i.pOpts.Enabled() && i.pCtx != nil {
		if err := i.pCtx.StopProfile(); err != nil {
			i.log.Error("unable to stop profile", zap.Error(err))
		}
	}
}

func (i *instrumentationContext) readStarted() {
	i.log.Info("read uninitialized start")
	i.span.LogFields(opentracinglog.String("event", "read_uninitialized_start"))
	i.start = i.nowFn()
	i.startProfileIfEnabled("uninitialized-read")
}

func (i *instrumentationContext) readCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapReadDuration.Record(duration)
	i.log.Info("read uninitialized done", zap.Duration("took", duration))
	i.span.LogFields(opentracinglog.String("event", "read_uninitialized_done"))
	i.stopProfileIfEnabled()
}

type instrumentation struct {
	pOpts profiler.Options
	scope tally.Scope
	log   *zap.Logger
	nowFn clock.NowFn
}

func newInstrumentation(opts Options) *instrumentation {
	var (
		scope = opts.InstrumentOptions().MetricsScope().SubScope("uninitialized-bootstrapper")
		iopts = opts.InstrumentOptions().SetMetricsScope(scope)
	)
	opts = opts.SetInstrumentOptions(iopts)

	return &instrumentation{
		pOpts: iopts.ProfilerOptions(),
		scope: scope,
		log:   iopts.Logger().With(zap.String("bootstrapper", "uninitialized")),
		nowFn: opts.ResultOptions().ClockOptions().NowFn(),
	}
}

func (i *instrumentation) uninitializedBootstrapperSourceReadStarted(
	ctx context.Context,
) *instrumentationContext {
	_, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperUninitializedSourceRead)
	return newInstrumentationContext(
		i.nowFn,
		i.log,
		span,
		i.scope,
		i.pOpts,
	)
}
