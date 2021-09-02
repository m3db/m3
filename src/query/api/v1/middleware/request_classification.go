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
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/x/headers"
)

const (
	// promDefaultLookback is the default lookback in Prometheus, that affects
	// the actual range it fetches datapoints from.
	promDefaultLookback = time.Minute * 5
)

type classificationMetrics struct {
	classifiedResult   tally.Counter
	classifiedDuration tally.Counter
	notClassifiable    tally.Counter
	badParams          tally.Counter
	error              tally.Counter
}

func newClassificationMetrics(scope tally.Scope) *classificationMetrics {
	buildCounter := func(status string) tally.Counter {
		return scope.Tagged(map[string]string{"status": status}).Counter("count")
	}

	return &classificationMetrics{
		classifiedResult:   buildCounter("classified_result"),
		classifiedDuration: buildCounter("classified_duration"),
		notClassifiable:    buildCounter("not_classifiable"),
		badParams:          buildCounter("bad_params"),
		error:              buildCounter("error"),
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

type classificationTags map[string]string

const (
	resultsClassification  = "results_bucket"
	durationClassification = "duration_bucket"
	unclassified           = "unclassified"
)

func newClassificationTags() classificationTags {
	return map[string]string{
		resultsClassification:  unclassified,
		durationClassification: unclassified,
	}
}

// classifyRequest determines the bucket(s) that important request metrics
// will fall into. Supported request metrics at this time are the number of results
// fetched and the time range of the request. The number of buckets and their values
// can be configured by the client. Currently, only query (i.e. query and query_range)
// and label (i.e. label names and label values) endpoints are supported.
func classifyRequest(
	w http.ResponseWriter,
	r *http.Request,
	metrics *classificationMetrics,
	opts Options,
	requestStart time.Time,
	path string,
) classificationTags {
	var (
		cfg  = opts.Metrics.Config
		tags = newClassificationTags()
	)

	if opts.Metrics.ParseQueryParams == nil {
		metrics.notClassifiable.Inc(1)
		return tags
	}
	params, err := opts.Metrics.ParseQueryParams(r, requestStart)
	if err != nil {
		metrics.badParams.Inc(1)
		return tags
	}

	// NB(nate): have to check for /label/*/values this way since the URL is templated
	labelValues := strings.Contains(path, "/label/") && strings.Contains(path, "/values")
	if path == route.QueryRangeURL || path == route.QueryURL {
		if params.Query == "" {
			metrics.badParams.Inc(1)
			return tags
		}

		if tags, err = classifyForQueryEndpoints(
			cfg.QueryEndpointsClassification, params, w, metrics,
		); err != nil {
			metrics.error.Inc(1)
			return tags
		}
	} else if path == route.LabelNamesURL || labelValues {
		if tags, err = classifyForLabelEndpoints(
			cfg.LabelEndpointsClassification, params, w, metrics,
		); err != nil {
			metrics.error.Inc(1)
			return tags
		}
	}

	return tags
}

func classifyForQueryEndpoints(
	cfg config.QueryClassificationConfig,
	params QueryParams,
	w http.ResponseWriter,
	metrics *classificationMetrics,
) (classificationTags, error) {
	resultsBuckets := cfg.ResultsBuckets
	tags := newClassificationTags()
	if len(resultsBuckets) > 0 {
		fetchedCount := w.Header().Get(headers.FetchedSeriesCount)
		fetched, err := strconv.Atoi(fetchedCount)
		if err != nil {
			return newClassificationTags(), err
		}

		tags[resultsClassification] = strconv.Itoa(resultsBuckets[0])
		for _, bucket := range resultsBuckets {
			if fetched >= bucket {
				tags[resultsClassification] = strconv.Itoa(bucket)
			}
		}
		metrics.classifiedResult.Inc(1)
	}

	durationBuckets := cfg.DurationBuckets
	if len(durationBuckets) > 0 {
		expr, err := parser.ParseExpr(params.Query)
		if err != nil {
			return newClassificationTags(), err
		}

		queryRange := retrieveQueryRange(expr, 0)
		duration := params.Range()
		totalDuration := duration + queryRange

		tags[durationClassification] = durationBuckets[0].String()
		for _, bucket := range durationBuckets {
			if totalDuration >= bucket {
				tags[durationClassification] = bucket.String()
			}
		}
		metrics.classifiedDuration.Inc(1)
	}

	return tags, nil
}

func classifyForLabelEndpoints(
	cfg config.QueryClassificationConfig,
	params QueryParams,
	w http.ResponseWriter,
	metrics *classificationMetrics,
) (classificationTags, error) {
	resultsBuckets := cfg.ResultsBuckets
	tags := newClassificationTags()
	if len(resultsBuckets) > 0 {
		fetchedCount := w.Header().Get(headers.FetchedMetadataCount)
		fetched, err := strconv.Atoi(fetchedCount)
		if err != nil {
			return newClassificationTags(), err
		}

		tags[resultsClassification] = strconv.Itoa(resultsBuckets[0])
		for _, bucket := range resultsBuckets {
			if fetched >= bucket {
				tags[resultsClassification] = strconv.Itoa(bucket)
			}
		}
		metrics.classifiedResult.Inc(1)
	}

	durationBuckets := cfg.DurationBuckets
	if len(durationBuckets) > 0 {
		duration := params.Range()
		tags[durationClassification] = durationBuckets[0].String()
		for _, bucket := range durationBuckets {
			if duration >= bucket {
				tags[durationClassification] = bucket.String()
			}
		}
		metrics.classifiedDuration.Inc(1)
	}

	return tags, nil
}
