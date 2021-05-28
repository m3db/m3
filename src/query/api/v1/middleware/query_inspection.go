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
	largeQuery          tally.Counter
}

func newQueryInspectionMetrics(scope tally.Scope) *queryInspectionMetrics {
	buildCounter := func(status string) tally.Counter {
		return scope.Tagged(map[string]string{"status": status}).Counter("count")
	}

	return &queryInspectionMetrics{
		notQuery:            buildCounter("not_query"),
		notInspected:        buildCounter("not_inspected"),
		belowCountThreshold: buildCounter("below_count_threshold"),
		badQuery:            buildCounter("bad_query"),
		belowRangeThreshold: buildCounter("below_range_threshold"),
		largeQuery:          buildCounter("large_query"),
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
			d := retrieveQueryRange(n.Expr, n.Range+n.OriginalOffset+rangeSoFar)
			if d > queryRange {
				queryRange = d
			}
		case *parser.MatrixSelector:
			useLookback = false
			// nolint
			v := n.VectorSelector.(*parser.VectorSelector)
			if d := v.OriginalOffset + n.Range + rangeSoFar; d > queryRange {
				queryRange = d
			}
		case *parser.VectorSelector:
			if d := n.OriginalOffset + rangeSoFar; d > queryRange {
				queryRange = d
			}
		}

		return nil
	})

	if useLookback {
		// NB: no range provided in any query selectors; use lookback time.
		queryRange += promDefaultLookback
	}

	return queryRange
}

func lastPathSegment(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx < 0 {
		return ""
	}

	return path[idx+1:]
}

type querySize struct {
	size           string
	countThreshold string
	rangeThreshold string
}

func (s querySize) toRouteMetricKey(path string, status int) routeMetricKey {
	return routeMetricKey{
		path:   path,
		size:   s.size,
		status: status,
	}
}

func (s querySize) toTags() map[string]string {
	return map[string]string{
		querySizeMetricName:      s.size,
		countThresholdMetricName: s.countThreshold,
		rangeThresholdMetricName: s.rangeThreshold,
	}
}

// NB: inspectQuerySize determines this query's size; if the number of fetched
// series exceeds a certain number, and the query range exceeds a certain range,
// the query is classified as "large"; otherwise, this query is "small".
func inspectQuerySize(
	w http.ResponseWriter,
	r *http.Request,
	path string,
	metrics *queryInspectionMetrics,
	cfg *config.MiddlewareConfiguration,
) querySize {
	size := querySize{
		size:           querySizeSmall,
		countThreshold: "0",
		rangeThreshold: "0",
	}

	duration := time.Duration(0)
	// NB: query categorization is only relevant for query and query_range endpoints.
	lastPath := lastPathSegment(path)
	if lastPath == "query_range" {
		// NB: for a query range, add the length of the range to the query duration
		// to get the full time range.
		if err := r.ParseForm(); err != nil {
			// NB: invalid query.
			metrics.badQuery.Inc(1)
			return size
		}

		now := time.Now()
		start, err := native.ParseTime(r, "start", now)
		if err != nil {
			// NB: invalid query.
			metrics.badQuery.Inc(1)
			return size
		}

		end, err := native.ParseTime(r, "end", now)
		if err != nil {
			// NB: invalid query.
			metrics.badQuery.Inc(1)
			return size
		}

		duration = end.Sub(start)
	} else if lastPath != "query" {
		metrics.notQuery.Inc(1)
		return size
	}

	// NB: query inspection is disabled
	if cfg == nil || !cfg.InspectQuerySize {
		metrics.notInspected.Inc(1)
		return size
	}

	size.countThreshold = fmt.Sprint(cfg.LargeSeriesCountThreshold)
	size.rangeThreshold = cfg.LargeSeriesRangeThreshold.String()

	fetchedCount := w.Header().Get(headers.FetchedSeriesCount)
	fetched, err := strconv.Atoi(fetchedCount)
	if err != nil || fetched < cfg.LargeSeriesCountThreshold {
		// NB: header does not exist, or value is below the large query threshold.
		metrics.belowCountThreshold.Inc(1)
		return size
	}

	query, err := native.ParseQuery(r)
	if err != nil {
		metrics.badQuery.Inc(1)
		return size
	}

	expr, err := parser.ParseExpr(query)
	if err != nil {
		metrics.badQuery.Inc(1)
		return size
	}

	queryRange := retrieveQueryRange(expr, 0)
	if duration+queryRange < cfg.LargeSeriesRangeThreshold {
		metrics.belowRangeThreshold.Inc(1)
		return size
	}

	metrics.largeQuery.Inc(1)
	size.size = querySizeLarge
	return size
}
