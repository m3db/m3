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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/x/headers"
)

const (
	querySizeMetricName      = "size"
	countThresholdMetricName = "count_threshold"
	rangeThresholdMetricName = "range_threshold"
	querySizeSmall           = "small"
	querySizeLarge           = "large"

	// promDefaultLookback is the default lookback in Prometheus, that affects
	// the actual range it fetches datapoints from.
	promDefaultLookback = time.Minute * 5
)

type queryInspectionMetrics struct {
	notQuery            tally.Counter
	notInspected        tally.Counter
	belowCountThreshold tally.Counter
	badQuery            tally.Counter
	belowRangeThreshold tally.Counter
	largeTags           tally.Counter
}

func newQueryInspectionMetrics(scope tally.Scope) queryInspectionMetrics {
	buildCounter := func(status string) tally.Counter {
		return scope.Tagged(map[string]string{"status": status}).Counter("count")
	}

	return queryInspectionMetrics{
		notQuery:            buildCounter("notQuery"),
		notInspected:        buildCounter("notInspected"),
		belowCountThreshold: buildCounter("belowCountThreshold"),
		badQuery:            buildCounter("badQuery"),
		belowRangeThreshold: buildCounter("belowRangeThreshold"),
		largeTags:           buildCounter("largeTags"),
	}
}

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

func isQueryPath(path string) bool {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return false
	}

	path = path[idx+1:]
	return path == "query" || path == "query_range"
}

// NB: inspectQuerySize determines this query's size; if the number of fetched
// series exceeds a certain number, and the query range exceeds a certain range,
// the query is classified as "large"; otherwise, this query is "small".
func inspectQuerySize(
	w http.ResponseWriter,
	r *http.Request,
	path string,
	metrics queryInspectionMetrics,
	cfg *config.MiddlewareConfiguration,
) map[string]string {
	tags := map[string]string{
		querySizeMetricName:      querySizeSmall,
		countThresholdMetricName: "0",
		rangeThresholdMetricName: "0",
	}

	// NB: query categorization is only relevant for query and query_range endpoints.
	if !isQueryPath(path) {
		metrics.notQuery.Inc(1)
		return tags
	}

	// NB: query inspection is disabled
	if cfg == nil || !cfg.InspectQuerySize {
		metrics.notInspected.Inc(1)
		return tags
	}

	tags[countThresholdMetricName] = fmt.Sprint(cfg.LargeSeriesCountThreshold)
	tags[rangeThresholdMetricName] = cfg.LargeSeriesRangeThreshold.String()

	fetchedCount := w.Header().Get(headers.FetchedSeriesCount)
	fetched, err := strconv.Atoi(fetchedCount)
	if err != nil || fetched < cfg.LargeSeriesCountThreshold {
		// NB: header does not exist, or value is below the large query threshold.
		metrics.belowCountThreshold.Inc(1)
		return tags
	}

	query, err := native.ParseQuery(r)
	if err != nil {
		metrics.badQuery.Inc(1)
		return tags
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		metrics.badQuery.Inc(1)
		return tags
	}

	queryRange := retrieveQueryRange(expr, 0)
	if queryRange < cfg.LargeSeriesRangeThreshold {
		metrics.belowRangeThreshold.Inc(1)
		return tags
	}

	metrics.largeTags.Inc(1)
	tags[querySizeMetricName] = fmt.Sprint(querySizeLarge)
	return tags
}
