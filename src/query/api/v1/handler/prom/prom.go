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

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/prometheus"
	"github.com/prometheus/prometheus/promql"
)

// Options defines options for PromQL handler.
type Options struct {
	PromQLEngine *promql.Engine
}

// NewReadHandler creates a handler to handle PromQL requests.
func NewReadHandler(opts Options, hOpts options.HandlerOptions) http.Handler {
	queryable := prometheus.NewPrometheusQueryable(
		prometheus.PrometheusOptions{
			Storage:           hOpts.Storage(),
			InstrumentOptions: hOpts.InstrumentOpts(),
		})
	return newReadHandler(opts, hOpts, queryable)
}

// NewReadInstantHandler creates a handler to handle PromQL requests.
func NewReadInstantHandler(opts Options, hOpts options.HandlerOptions) http.Handler {
	queryable := prometheus.NewPrometheusQueryable(
		prometheus.PrometheusOptions{
			Storage:           hOpts.Storage(),
			InstrumentOptions: hOpts.InstrumentOpts(),
		})
	return newReadInstantHandler(opts, hOpts, queryable)
}
