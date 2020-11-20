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
	newQueryFn   NewQueryFn
}

type Option interface {
	apply(*opts) error
}

func WithEngine(promQLEngine *promql.Engine) Option {
	return instantEngineOption{promQLEngine: promQLEngine, instant: false}
}

func WithInstantEngine(promQLEngine *promql.Engine) Option {
	return instantEngineOption{promQLEngine: promQLEngine, instant: true}
}

type instantEngineOption struct {
	promQLEngine *promql.Engine
	instant      bool
}

func (o instantEngineOption) apply(options *opts) error {
	if o.promQLEngine == nil {
		return errors.New("invalid engine")
	}
	options.instant = o.instant
	options.promQLEngine = o.promQLEngine
	options.newQueryFn = newRangeQueryFn
	if o.instant {
		options.newQueryFn = newInstantQueryFn
	}
	return nil
}

func newDefaultOptions(hOpts options.HandlerOptions) opts {
	return opts{
		promQLEngine: hOpts.PrometheusEngine(),
		instant:      false,
		newQueryFn:   newRangeQueryFn,
	}
}

// NewReadHandler creates a handler to handle PromQL requests.
func NewReadHandler(hOpts options.HandlerOptions, options ...Option) (http.Handler, error) {
	return NewReadHandlerWithHooks(hOpts, &noopReadHandlerHooks{}, options...)
}

// NewReadHandlerWithHooks creates a handler for PromQL requests that accepts ReadHandlerHooks.
func NewReadHandlerWithHooks(
	hOpts options.HandlerOptions,
	hooks ReadHandlerHooks,
	options ...Option,
) (http.Handler, error) {
	queryable := prometheus.NewPrometheusQueryable(
		prometheus.PrometheusOptions{
			Storage:           hOpts.Storage(),
			InstrumentOptions: hOpts.InstrumentOpts(),
		})

	return newReadHandler(hOpts, hooks, queryable, options...)
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
