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

package peers

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
)

type instrumentationContext struct {
	nowFn                  clock.NowFn
	log                    *zap.Logger
	start                  time.Time
	span                   opentracing.Span
	bootstrapDataDuration  tally.Timer
	bootstrapIndexDuration tally.Timer
}

func newInstrumentationContext(
	nowFn clock.NowFn,
	log *zap.Logger,
	span opentracing.Span,
	scope tally.Scope,
) *instrumentationContext {
	return &instrumentationContext{
		nowFn:                  nowFn,
		log:                    log,
		span:                   span,
		bootstrapDataDuration:  scope.Timer("data-duration"),
		bootstrapIndexDuration: scope.Timer("index-duration"),
	}
}

func (i *instrumentationContext) finish() {
	i.span.Finish()
}

func (i *instrumentationContext) bootstrapDataStarted() {
	i.log.Info("bootstrapping time series data start")
	i.span.LogFields(opentracinglog.String("event", "bootstrap_data_start"))
	i.start = i.nowFn()
}

func (i *instrumentationContext) bootstrapDataCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapDataDuration.Record(duration)
	i.log.Info("bootstrapping time series data success", zap.Duration("took", duration))
	i.span.LogFields(opentracinglog.String("event", "bootstrap_data_done"))
}

func (i *instrumentationContext) bootstrapIndexStarted() {
	i.log.Info("bootstrapping index metadata start")
	i.span.LogFields(opentracinglog.String("event", "bootstrap_index_start"))
	i.start = i.nowFn()
}

func (i *instrumentationContext) bootstrapIndexCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapIndexDuration.Record(duration)
	i.log.Info("bootstrapping index metadata success", zap.Duration("took", duration))
	i.span.LogFields(opentracinglog.String("event", "bootstrap_index_done"))
}

type instrumentationReadShardsContext struct {
	nowFn                   clock.NowFn
	log                     *zap.Logger
	start                   time.Time
	bootstrapShardsDuration tally.Timer
}

func newInstrumentationReadShardsContext(
	nowFn clock.NowFn,
	log *zap.Logger,
	scope tally.Scope,
) *instrumentationReadShardsContext {
	return &instrumentationReadShardsContext{
		nowFn:                   nowFn,
		log:                     log,
		start:                   nowFn(),
		bootstrapShardsDuration: scope.Timer("shards-duration"),
	}
}

func (i *instrumentationReadShardsContext) bootstrapShardsCompleted() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapShardsDuration.Record(duration)
	i.log.Info("bootstrapping shards success", zap.Duration("took", duration))
}

type instrumentation struct {
	opts                               Options
	scope                              tally.Scope
	log                                *zap.Logger
	nowFn                              clock.NowFn
	persistedIndexBlocksOutOfRetention tally.Counter
}

func newInstrumentation(opts Options) *instrumentation {
	var (
		scope = opts.ResultOptions().InstrumentOptions().
			MetricsScope().SubScope("peers-bootstrapper")
		instrumentOptions = opts.ResultOptions().InstrumentOptions().SetMetricsScope(scope)
	)

	return &instrumentation{
		opts:                               opts,
		scope:                              scope,
		log:                                instrumentOptions.Logger().With(zap.String("bootstrapper", "peers")),
		nowFn:                              opts.ResultOptions().ClockOptions().NowFn(),
		persistedIndexBlocksOutOfRetention: scope.Counter("persist-index-blocks-out-of-retention"),
	}
}

func (i *instrumentation) peersBootstrapperSourceReadStarted(
	ctx context.Context,
) *instrumentationContext {
	_, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperPeersSourceRead)
	return newInstrumentationContext(
		i.nowFn,
		i.log,
		span,
		i.scope,
	)
}

func (i *instrumentation) bootstrapShardsStarted(
	count int,
	concurrency int,
	shouldPersist bool,
) *instrumentationReadShardsContext {
	i.log.Info("peers bootstrapper bootstrapping shards for ranges",
		zap.Int("shards", count),
		zap.Int("concurrency", concurrency),
		zap.Bool("shouldPersist", shouldPersist))
	return newInstrumentationReadShardsContext(
		i.nowFn,
		i.log,
		i.scope,
	)
}

func (i *instrumentation) outOfRetentionIndexSegmentSkipped(fields []zapcore.Field) {
	i.log.Debug("skipping out of retention index segment", fields...)
	i.persistedIndexBlocksOutOfRetention.Inc(1)
}
