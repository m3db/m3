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
	"github.com/m3db/m3collector/metric"
	"github.com/m3db/m3collector/rules"
	"github.com/m3db/m3x/errors"
)

// Reporter reports aggregated metrics.
type Reporter interface {
	// ReportCounter reports a counter metric.
	ReportCounter(id metric.ID, value int64) error

	// ReportBatchTimer reports a batched timer metric.
	ReportBatchTimer(id metric.ID, value []float64) error

	// ReportGauge reports a gauge metric.
	ReportGauge(id metric.ID, value float64) error

	// Flush flushes any buffered metrics.
	Flush() error

	// Close closes the reporter.
	Close() error
}

type reporter struct {
	matcher rules.Matcher
	server  backend.Server
}

// NewReporter creates a new reporter.
func NewReporter(matcher rules.Matcher, server backend.Server) Reporter {
	return &reporter{
		matcher: matcher,
		server:  server,
	}
}

func (r *reporter) ReportCounter(id metric.ID, value int64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	if err := r.server.WriteCounterWithPolicies(id.Bytes(), value, matchRes.Mappings()); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollupID, rollupPolicies := matchRes.Rollups(idx)
		if err := r.server.WriteCounterWithPolicies(rollupID, value, rollupPolicies); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (r *reporter) ReportBatchTimer(id metric.ID, value []float64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	if err := r.server.WriteBatchTimerWithPolicies(id.Bytes(), value, matchRes.Mappings()); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollupID, rollupPolicies := matchRes.Rollups(idx)
		if err := r.server.WriteBatchTimerWithPolicies(rollupID, value, rollupPolicies); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (r *reporter) ReportGauge(id metric.ID, value float64) error {
	multiErr := xerrors.NewMultiError()
	matchRes := r.matcher.Match(id)
	if err := r.server.WriteGaugeWithPolicies(id.Bytes(), value, matchRes.Mappings()); err != nil {
		multiErr = multiErr.Add(err)
	}
	for idx := 0; idx < matchRes.NumRollups(); idx++ {
		rollupID, rollupPolicies := matchRes.Rollups(idx)
		if err := r.server.WriteGaugeWithPolicies(rollupID, value, rollupPolicies); err != nil {
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
