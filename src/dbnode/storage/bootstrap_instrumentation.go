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
	"github.com/m3db/m3/src/x/instrument"
)

type instrumentationContext struct {
	start                       time.Time
	log                         *zap.Logger
	logFields                   []zapcore.Field
	bootstrapDuration           tally.Timer
	bootstrapNamespacesDuration tally.Timer
	nowFn                       clock.NowFn
	instrumentOptions           instrument.Options
}

func newInstrumentationContext(
	nowFn clock.NowFn,
	log *zap.Logger,
	scope tally.Scope,
	opts Options,
) *instrumentationContext {
	return &instrumentationContext{
		start:                       nowFn(),
		log:                         log,
		nowFn:                       nowFn,
		bootstrapDuration:           scope.Timer("bootstrap-duration"),
		bootstrapNamespacesDuration: scope.Timer("bootstrap-namespaces-duration"),
		instrumentOptions:           opts.InstrumentOptions(),
	}
}

func (i *instrumentationContext) bootstrapStarted(shards int) {
	i.logFields = append(i.logFields, zap.Int("numShards", shards))
	i.log.Info("bootstrap started", i.logFields...)
}

func (i *instrumentationContext) bootstrapSucceeded() {
	bootstrapDuration := i.nowFn().Sub(i.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Info("bootstrap succeeded, marking namespaces complete", i.logFields...)
}

func (i *instrumentationContext) bootstrapFailed(err error) {
	bootstrapDuration := i.nowFn().Sub(i.start)
	i.bootstrapDuration.Record(bootstrapDuration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapDuration", bootstrapDuration))
	i.log.Error("bootstrap failed", append(i.logFields, zap.Error(err))...)
}

func (i *instrumentationContext) bootstrapNamespacesStarted() {
	i.start = i.nowFn()
	i.log.Info("bootstrap namespaces start", i.logFields...)
}

func (i *instrumentationContext) bootstrapNamespacesSucceeded() {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapNamespacesDuration.Record(duration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces success", i.logFields...)
}

func (i *instrumentationContext) bootstrapNamespaceFailed(
	err error,
	namespaceID ident.ID,
) {
	i.log.Info("bootstrap namespace error", append(i.logFields,
		zap.String("namespace", namespaceID.String()),
		zap.Error(err))...)
}

func (i *instrumentationContext) bootstrapNamespacesFailed(err error) {
	duration := i.nowFn().Sub(i.start)
	i.bootstrapNamespacesDuration.Record(duration)
	i.logFields = append(i.logFields, zap.Duration("bootstrapNamespacesDuration", duration))
	i.log.Info("bootstrap namespaces failed", append(i.logFields, zap.Error(err))...)
}

func (i *instrumentationContext) emitAndLogInvariantViolation(err error, msg string) {
	instrument.EmitAndLogInvariantViolation(i.instrumentOptions, func(l *zap.Logger) {
		l.Error(msg, append(i.logFields, zap.Error(err))...)
	})
}

type bootstrapInstrumentation struct {
	opts          Options
	scope         tally.Scope
	log           *zap.Logger
	nowFn         clock.NowFn
	status        tally.Gauge
	durableStatus tally.Gauge
	numRetries    tally.Counter
}

func newBootstrapInstrumentation(opts Options) *bootstrapInstrumentation {
	scope := opts.InstrumentOptions().MetricsScope()
	return &bootstrapInstrumentation{
		opts:          opts,
		scope:         scope,
		log:           opts.InstrumentOptions().Logger(),
		nowFn:         opts.ClockOptions().NowFn(),
		status:        scope.Gauge("bootstrapped"),
		durableStatus: scope.Gauge("bootstrapped-durable"),
		numRetries:    scope.Counter("bootstrap-retries"),
	}
}

func (i *bootstrapInstrumentation) bootstrapPreparing() *instrumentationContext {
	i.log.Info("bootstrap prepare")
	return newInstrumentationContext(i.nowFn, i.log, i.scope, i.opts)
}

func (i *bootstrapInstrumentation) bootstrapFailed(retry int) {
	i.numRetries.Inc(1)
	i.log.Warn("retrying bootstrap after backoff",
		zap.Duration("backoff", bootstrapRetryInterval),
		zap.Int("numRetries", retry))
}

func (i *bootstrapInstrumentation) bootstrapPrepareFailed(err error) {
	i.log.Error("bootstrap prepare failed", zap.Error(err))
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
