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

package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/x/headers"
)

const (
	querySizeMetricName = "size"
	querySizeSmall      = "small"
	querySizeLarge      = "large"

	// promDefaultLookback is the default lookback in Prometheus, that affects
	// the actual range it fetches datapoints from.
	promDefaultLookback = time.Minute * 5
)

func retrieveQueryRange(expr parser.Node, rangeSoFar time.Duration) time.Duration {
	var (
		queryRange  = rangeSoFar
		useLookback = rangeSoFar == 0
	)

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		switch n := node.(type) {
		case *parser.SubqueryExpr:
			useLookback = false
			d := retrieveQueryRange(n.Expr, n.Range+n.Offset+rangeSoFar)
			if d > queryRange {
				queryRange = d
			}
		case *parser.MatrixSelector:
			useLookback = false
			// nolint
			v := n.VectorSelector.(*parser.VectorSelector)
			if d := v.Offset + n.Range + rangeSoFar; d > queryRange {
				queryRange = d
			}
		case *parser.VectorSelector:
			if d := n.Offset + rangeSoFar; d > queryRange {
				queryRange = d
			}
		}

		return nil
	})

	if useLookback {
		// NB: no range provided in any query selecturs; use lookback time.
		queryRange += promDefaultLookback
	}

	return queryRange
}

// NB: calculateQuerySize determines this query's size; if the number of fetched
// series exceeds a certain number, and the query range exceeds a certain range,
// the query is classified as "large"; otherwise, this query is "small".
func calculateQuerySize(
	w http.ResponseWriter,
	r *http.Request,
	cfg *config.MiddlewareConfiguration,
) string {
	if cfg == nil || !cfg.InspectQuerySize {
		// NB: query inspection is disabled.
		return querySizeSmall
	}

	fetchedCount := w.Header().Get(headers.FetchedSeriesCount)
	fetched, err := strconv.Atoi(fetchedCount)
	if err != nil || fetched < int(cfg.LargeSeriesCountThreshold) {
		// NB: header does not exist, or value is below the large query threshold.
		// Classify this as a small query.
		return querySizeSmall
	}

	query, err := native.ParseQuery(r)
	if err != nil {
		return querySizeSmall
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		return querySizeSmall
	}

	queryRange := retrieveQueryRange(expr, 0)
	if queryRange < cfg.LargeSeriesRangeThreshold {
		return querySizeSmall
	}

	return querySizeLarge
}
