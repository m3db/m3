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
}

func newInstrumentationContext(
	nowFn clock.NowFn,
	log *zap.Logger,
	span opentracing.Span,
	scope tally.Scope,
) *instrumentationContext {
	ctx := &instrumentationContext{
		nowFn:                 nowFn,
		log:                   log,
		span:                  span,
		bootstrapReadDuration: scope.Timer("read-duration"),
	}
	ctx.readStarted()
	return ctx
}

func (i *instrumentationContext) finish() {
	i.readCompleted()
	i.span.Finish()
}

func (i *instrumentationContext) readStarted() {
	i.log.Info("read uninitialized start")
	i.span.LogFields(opentracinglog.String("event", "read_uninitialized_start"))
	i.start = i.nowFn()
}

func (i *instrumentationContext) readCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapReadDuration.Record(duration)
	i.log.Info("read uninitialized done", zap.Duration("took", duration))
	i.span.LogFields(opentracinglog.String("event", "read_uninitialized_done"))
}

type instrumentation struct {
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
	)
}
