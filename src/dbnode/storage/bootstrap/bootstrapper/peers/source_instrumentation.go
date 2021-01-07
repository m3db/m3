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

	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
)

type instrumentationContext struct {
	start time.Time
}

type instrumentation struct {
	opts                               Options
	log                                *zap.Logger
	nowFn                              clock.NowFn
	bootstrapDataDuration              tally.Timer
	bootstrapIndexDuration             tally.Timer
	bootstrapShardsDuration            tally.Timer
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
		log:                                instrumentOptions.Logger().With(zap.String("bootstrapper", "peers")),
		nowFn:                              opts.ResultOptions().ClockOptions().NowFn(),
		bootstrapDataDuration:              scope.Timer("data-duration"),
		bootstrapIndexDuration:             scope.Timer("index-duration"),
		bootstrapShardsDuration:            scope.Timer("shards-duration"),
		persistedIndexBlocksOutOfRetention: scope.Counter("persist-index-blocks-out-of-retention"),
	}
}

func (i *instrumentation) bootstrapDataStarted(span opentracing.Span) *instrumentationContext {
	i.log.Info("bootstrapping time series data start")
	span.LogFields(opentracinglog.String("event", "bootstrap_data_start"))
	return &instrumentationContext{start: i.nowFn()}
}

func (i *instrumentation) bootstrapDataCompleted(ctx *instrumentationContext, span opentracing.Span) {
	duration := i.nowFn().Sub(ctx.start)
	i.bootstrapDataDuration.Record(duration)
	i.log.Info("bootstrapping time series data success", zap.Duration("took", duration))
	span.LogFields(opentracinglog.String("event", "bootstrap_data_done"))
}

func (i *instrumentation) bootstrapIndexStarted(span opentracing.Span) *instrumentationContext {
	i.log.Info("bootstrapping index metadata start")
	span.LogFields(opentracinglog.String("event", "bootstrap_index_start"))
	return &instrumentationContext{start: i.nowFn()}
}

func (i *instrumentation) bootstrapIndexCompleted(ctx *instrumentationContext, span opentracing.Span) {
	duration := i.nowFn().Sub(ctx.start)
	i.bootstrapIndexDuration.Record(duration)
	i.log.Info("bootstrapping index metadata success", zap.Duration("took", duration))
	span.LogFields(opentracinglog.String("event", "bootstrap_index_done"))
}

func (i *instrumentation) bootstrapIndexSkipped(namespaceID ident.ID) {
	i.log.Info("skipping bootstrap for namespace based on options",
		zap.Stringer("namespace", namespaceID))
}

func (i *instrumentation) getDefaultAdminSessionFailed(err error) {
	i.log.Error("peers bootstrapper cannot get default admin session", zap.Error(err))
}

func (i *instrumentation) bootstrapShardsStarted(
	count int,
	concurrency int,
	shouldPersist bool,
) *instrumentationContext {
	i.log.Info("peers bootstrapper bootstrapping shards for ranges",
		zap.Int("shards", count),
		zap.Int("concurrency", concurrency),
		zap.Bool("shouldPersist", shouldPersist))
	return &instrumentationContext{start: i.nowFn()}
}

func (i *instrumentation) bootstrapShardsCompleted(ctx *instrumentationContext) {
	duration := i.nowFn().Sub(ctx.start)
	i.bootstrapShardsDuration.Record(duration)
	i.log.Info("bootstrapping shards success", zap.Duration("took", duration))
}

func (i *instrumentation) persistenceFlushFailed(err error) {
	i.log.Error("peers bootstrapper bootstrap with persistence flush encountered error",
		zap.Error(err))
}

func (i *instrumentation) seriesCheckoutFailed(err error) {
	i.log.Error("could not checkout series", zap.Error(err))
}

func (i *instrumentation) seriesLoadFailed(err error) {
	i.log.Error("could not load series block", zap.Error(err))
}

func (i *instrumentation) shardBootstrapped(shard uint32, numSeries int64, blockTime time.Time) {
	i.log.Info("peer bootstrapped shard",
		zap.Uint32("shard", shard),
		zap.Int64("numSeries", numSeries),
		zap.Time("blockStart", blockTime),
	)
}

func (i *instrumentation) fetchBootstrapBlocksFailed(err error, shard uint32) {
	i.log.Error("error fetching bootstrap blocks",
		zap.Uint32("shard", shard),
		zap.Error(err),
	)
}

func (i *instrumentation) peersBootstrapperIndexForRanges(count int) {
	i.log.Info("peers bootstrapper bootstrapping index for ranges",
		zap.Int("shards", count),
	)
}

func (i *instrumentation) processingReadersFailed(err error, start time.Time) {
	i.log.Error("error processing readers", zap.Error(err),
		zap.Time("timeRange.start", start))
}

func (i *instrumentation) buildingFileSetIndexSegmentStarted(fields []zapcore.Field) {
	i.log.Debug("building file set index segment", fields...)
}

func (i *instrumentation) outOfRetentionIndexSegmentSkipped(fields []zapcore.Field) {
	i.log.Debug("skipping out of retention index segment", fields...)
	i.persistedIndexBlocksOutOfRetention.Inc(1)
}

func (i *instrumentation) buildingInMemoryIndexSegmentStarted(fields []zapcore.Field) {
	i.log.Info("building in-memory index segment", fields...)
}

func (i *instrumentation) errorsForRangeEncountered(summaryString string, errorsString []string) {
	i.log.Info("encountered errors for range",
		zap.String("requestedRanges", summaryString),
		zap.Strings("timesWithErrors", errorsString))
}

func (i *instrumentation) noPeersAvailable(total int, shardIDUint uint32) {
	i.log.Debug("0 available peers, unable to peer bootstrap",
		zap.Int("total", total),
		zap.Uint32("shard", shardIDUint))
}

func (i *instrumentation) readConsistencyNotAchieved(
	bootstrapConsistencyLevel topology.ReadConsistencyLevel,
	majorityReplicas int,
	total int,
	available int,
) {
	i.log.Debug("read consistency not achieved, unable to peer bootstrap",
		zap.Any("level", bootstrapConsistencyLevel),
		zap.Int("replicas", majorityReplicas),
		zap.Int("total", total),
		zap.Int("available", available))
}

func (i *instrumentation) peersBootstrapperSourceReadStarted(
	ctx context.Context,
) opentracing.Span {
	_, span, _ := ctx.StartSampledTraceSpan(tracepoint.BootstrapperPeersSourceRead)
	return span
}
