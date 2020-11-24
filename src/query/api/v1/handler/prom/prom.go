// Copyright (c) 2020 Uber Technologies, Inc.
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

package prom

import (
	"net/http"
	"time"

	promstorage "github.com/prometheus/prometheus/storage"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/storage/prometheus"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// NB: since Prometheus engine is not brought up in the usual fashion,
// default subquery evaluation interval is unset, causing div by 0 errors.
func init() {
	promql.SetDefaultEvaluationInterval(time.Minute)
}

// opts defines options for PromQL handler.
type opts struct {
	promQLEngine *promql.Engine
	instant      bool
	queryable    promstorage.Queryable
	newQueryFn   NewQueryFn
}

// Option is a Prometheus handler option.
type Option func(*opts) error

// WithEngine sets the PromQL engine.
func WithEngine(promQLEngine *promql.Engine) Option {
	return withEngine(promQLEngine, false)
}

// WithInstantEngine sets the PromQL instant engine.
func WithInstantEngine(promQLEngine *promql.Engine) Option {
	return withEngine(promQLEngine, true)
}

func withEngine(promQLEngine *promql.Engine, instant bool) Option {
	return func(o *opts) error {
		if promQLEngine == nil {
			return errors.New("invalid engine")
		}
		o.instant = instant
		o.promQLEngine = promQLEngine
		o.newQueryFn = newRangeQueryFn(promQLEngine, o.queryable)
		if instant {
			o.newQueryFn = newInstantQueryFn(promQLEngine, o.queryable)
		}
		return nil
	}
}

func newDefaultOptions(hOpts options.HandlerOptions) opts {
	queryable := prometheus.NewPrometheusQueryable(
		prometheus.PrometheusOptions{
			Storage:           hOpts.Storage(),
			InstrumentOptions: hOpts.InstrumentOpts(),
		})
	return opts{
		promQLEngine: hOpts.PrometheusEngine(),
		queryable:    queryable,
		instant:      false,
		newQueryFn:   newRangeQueryFn(hOpts.PrometheusEngine(), queryable),
	}
}

// NewReadHandler creates a handler to handle PromQL requests.
func NewReadHandler(hOpts options.HandlerOptions, options ...Option) (http.Handler, error) {
	opts := newDefaultOptions(hOpts)
	for _, optionFn := range options {
		if err := optionFn(&opts); err != nil {
			return nil, err
		}
	}
	return newReadHandler(hOpts, opts)
}

// ApplyRangeWarnings applies warnings encountered during execution.
func ApplyRangeWarnings(
	query string, meta *block.ResultMetadata,
) error {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return err
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if n, ok := node.(*parser.MatrixSelector); ok {
			meta.VerifyTemporalRange(n.Range)
		}

		return nil
	})

	return nil
}
