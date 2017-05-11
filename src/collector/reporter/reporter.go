// Copyright (c) 2017 Uber Technologies, Inc.
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

package reporter

import (
	"github.com/m3db/m3collector/backend"
	"github.com/m3db/m3collector/rules"
	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/errors"
)

// Reporter reports aggregated metrics.
type Reporter interface {
	// ReportCounter reports a counter metric.
	ReportCounter(id id.ID, value int64) error

	// ReportBatchTimer reports a batched timer metric.
	ReportBatchTimer(id id.ID, value []float64) error

	// ReportGauge reports a gauge metric.
	ReportGauge(id id.ID, value float64) error

	// Flush flushes any buffered metrics.
	Flush() error

	// Close closes the reporter.
	Close() error
}

type reporter struct {
	matcher rules.Matcher
	server  backend.Server
	nowFn   clock.NowFn
}

// NewReporter creates a new reporter.
func NewReporter(
	matcher rules.Matcher,
	server backend.Server,
	clockOpts clock.Options,
) Reporter {
	return &reporter{
		matcher: matcher,
		server:  server,
		nowFn:   clockOpts.NowFn(),
	}
}

func (r *reporter) ReportCounter(id id.ID, value int64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	now := r.nowFn()
	if err := r.server.WriteCounterWithPoliciesList(id.Bytes(), value, matchRes.MappingsAt(now)); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup := matchRes.RollupsAt(idx, now)
		if err := r.server.WriteCounterWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (r *reporter) ReportBatchTimer(id id.ID, value []float64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	now := r.nowFn()
	if err := r.server.WriteBatchTimerWithPoliciesList(id.Bytes(), value, matchRes.MappingsAt(now)); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup := matchRes.RollupsAt(idx, now)
		if err := r.server.WriteBatchTimerWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (r *reporter) ReportGauge(id id.ID, value float64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	now := r.nowFn()
	if err := r.server.WriteGaugeWithPoliciesList(id.Bytes(), value, matchRes.MappingsAt(now)); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollup := matchRes.RollupsAt(idx, now)
		if err := r.server.WriteGaugeWithPoliciesList(rollup.ID, value, rollup.PoliciesList); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (r *reporter) Flush() error {
	return r.server.Flush()
}

func (r *reporter) Close() error {
	multiErr := xerrors.NewMultiError()
	if err := r.server.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := r.matcher.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}
