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

package storage

import (
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/ident"
)

type instrumentationContext struct {
	start     time.Time
	logFields []zapcore.Field
}

type bootstrapInstrumentation struct {
	opts                        Options
	log                         *zap.Logger
	nowFn                       clock.NowFn
	status                      tally.Gauge
	bootstrapDuration           tally.Timer
	bootstrapNamespacesDuration tally.Timer
	durableStatus               tally.Gauge
}

func newInstrumentation(opts Options) *bootstrapInstrumentation {
	scope := opts.InstrumentOptions().MetricsScope()
	return &bootstrapInstrumentation{
		opts:                        opts,
		log:                         opts.InstrumentOptions().Logger(),
		nowFn:                       opts.ClockOptions().NowFn(),
		status:                      scope.Gauge("bootstrapped"),
		bootstrapDuration:           scope.Timer("bootstrap-duration"),
		bootstrapNamespacesDuration: scope.Timer("bootstrap-namespaces-duration"),
		durableStatus:               scope.Gauge("bootstrapped-durable"),
	}
}

func (i *bootstrapInstrumentation) bootstrapFnFailed(retry int) {
	i.log.Warn("retrying bootstrap after backoff",
		zap.Duration("backoff", bootstrapRetryInterval),
		zap.Int("numRetries", retry+1))
}

func (i *bootstrapInstrumentation) bootstrapPreparing() *instrumentationContext {
	i.log.Info("bootstrap prepare")
	return &instrumentationContext{
		start: i.nowFn(),
	}
}

func (i *bootstrapInstrumentation) bootstrapPrepareFailed(err error) {
	i.log.Error("bootstrap prepare failed", zap.Error(err))
}

func (i *bootstrapInstrumentation) bootstrapStarted(ctx *instrumentationContext, shards int) {
	ctx.logFields = append(ctx.logFields, zap.Int("numShards", shards))
	i.log.Info("bootstrap started", ctx.logFields...)
}

func (i *bootstrapInstrumentation) bootstrapSucceeded(ctx *instrumentationContext) {
	bootstrapDuration := i.nowFn().Sub(ctx.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	ctx.logFields = append(ctx.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Info("bootstrap succeeded, marking namespaces complete", ctx.logFields...)
}

func (i *bootstrapInstrumentation) bootstrapFailed(ctx *instrumentationContext, err error) {
	bootstrapDuration := i.nowFn().Sub(ctx.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	ctx.logFields = append(ctx.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Error("bootstrap failed", append(ctx.logFields, zap.Error(err))...)
}

func (i *bootstrapInstrumentation) bootstrapNamespaceFailed(
	ctx *instrumentationContext,
	err error,
	namespaceID ident.ID,
) {
	i.log.Info("bootstrap namespace error", append(ctx.logFields, []zapcore.Field{
		zap.String("namespace", namespaceID.String()),
		zap.Error(err),
	}...)...)
}

func (i *bootstrapInstrumentation) bootstrapNamespacesFailed(ctx *instrumentationContext, err error) {
	duration := i.nowFn().Sub(ctx.start)
	i.bootstrapNamespacesDuration.Record(duration)
	ctx.logFields = append(ctx.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces failed", append(ctx.logFields, zap.Error(err))...)
}

func (i *bootstrapInstrumentation) bootstrapNamespacesStarted(ctx *instrumentationContext) *instrumentationContext {
	i.log.Info("bootstrap namespaces start", ctx.logFields...)
	return &instrumentationContext{
		start:     i.nowFn(),
		logFields: ctx.logFields,
	}
}

func (i *bootstrapInstrumentation) bootstrapNamespacesSucceeded(ctx *instrumentationContext) {
	duration := i.nowFn().Sub(ctx.start)
	i.bootstrapNamespacesDuration.Record(duration)
	ctx.logFields = append(ctx.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces success", ctx.logFields...)
}

func (i *bootstrapInstrumentation) setIsBootstrapped(isBootstrapped bool) {
	var status float64 = 0
	if isBootstrapped {
		status = 1
	}
	i.status.Update(status)
}

func (i *bootstrapInstrumentation) setIsBootstrappedAndDurable(isBootstrappedAndDurable bool) {
	var status float64 = 0
	if isBootstrappedAndDurable {
		status = 1
	}
	i.durableStatus.Update(status)
}
