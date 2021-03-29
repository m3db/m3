// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	comparator "github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	xpromql "github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/pkg/labels"
	promql "github.com/prometheus/prometheus/promql/parser"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	// PromReadURL is the url for remote prom read handler
	PromReadURL = handler.RoutePrefixV1 + "/prom/remote/read"
)

var (
	// PromReadHTTPMethods are the HTTP methods used with this resource.
	PromReadHTTPMethods = []string{http.MethodPost, http.MethodGet}
)

// promReadHandler is a handler for the prometheus remote read endpoint.
type promReadHandler struct {
	promReadMetrics promReadMetrics
	opts            options.HandlerOptions
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler(opts options.HandlerOptions) http.Handler {
	taggedScope := opts.InstrumentOpts().MetricsScope().
		Tagged(map[string]string{"handler": "remote-read"})
	return &promReadHandler{
		promReadMetrics: newPromReadMetrics(taggedScope),
		opts:            opts,
	}
}

type promReadMetrics struct {
	fetchSuccess      tally.Counter
	fetchErrorsServer tally.Counter
	fetchErrorsClient tally.Counter
	fetchTimerSuccess tally.Timer
}

func newPromReadMetrics(scope tally.Scope) promReadMetrics {
	return promReadMetrics{
		fetchSuccess: scope.
			Counter("fetch.success"),
		fetchErrorsServer: scope.Tagged(map[string]string{"code": "5XX"}).
			Counter("fetch.errors"),
		fetchErrorsClient: scope.Tagged(map[string]string{"code": "4XX"}).
			Counter("fetch.errors"),
		fetchTimerSuccess: scope.Timer("fetch.success.latency"),
	}
}

func (m *promReadMetrics) incError(err error) {
	if xhttp.IsClientError(err) {
		m.fetchErrorsClient.Inc(1)
	} else {
		m.fetchErrorsServer.Inc(1)
	}
}

func (h *promReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	timer := h.promReadMetrics.fetchTimerSuccess.Start()
	defer timer.Stop()

	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.opts.InstrumentOpts())
	req, fetchOpts, rErr := ParseRequest(ctx, r, h.opts)
	if rErr != nil {
		h.promReadMetrics.incError(rErr)
		logger.Error("remote read query parse error",
			zap.Error(rErr),
			zap.Any("req", req),
			zap.Any("fetchOpts", fetchOpts))
		xhttp.WriteError(w, rErr)
		return
	}

	cancelWatcher := handler.NewResponseWriterCanceller(w, h.opts.InstrumentOpts())
	readResult, err := Read(ctx, cancelWatcher, req, fetchOpts, h.opts)
	if err != nil {
		h.promReadMetrics.incError(err)
		logger.Error("remote read query error",
			zap.Error(err),
			zap.Any("req", req),
			zap.Any("fetchOpts", fetchOpts))
		xhttp.WriteError(w, err)
		return
	}

	// Write headers before response.
	handleroptions.AddResponseHeaders(w, readResult.Meta, fetchOpts)

	// NB: if this errors, all relevant headers and information should already
	// be sent to the writer; so it is not necessary to do anything here other
	// than increment success/failure metrics.
	switch r.FormValue("format") {
	case "json":
		result := readResultsJSON{
			Queries: make([]queryResultsJSON, 0, len(req.Queries)),
		}
		for i, q := range req.Queries {
			start := storage.PromTimestampToTime(q.StartTimestampMs)
			end := storage.PromTimestampToTime(q.EndTimestampMs)

			all := readResult.Result[i].Timeseries
			timeseries := make([]comparator.Series, 0, len(all))
			for _, s := range all {
				datapoints := storage.PromSamplesToM3Datapoints(s.Samples)
				tags := storage.PromLabelsToM3Tags(s.Labels, h.opts.TagOptions())
				series := toSeries(datapoints, tags)
				series.Start = start
				series.End = end
				timeseries = append(timeseries, series)
			}

			matchers := make([]labelMatcherJSON, 0, len(q.Matchers))
			for _, m := range q.Matchers {
				matcher := labelMatcherJSON{
					Type:  m.Type.String(),
					Name:  string(m.Name),
					Value: string(m.Value),
				}
				matchers = append(matchers, matcher)
			}

			result.Queries = append(result.Queries, queryResultsJSON{
				Query: queryJSON{
					Matchers: matchers,
				},
				Start:  start,
				End:    end,
				Series: timeseries,
			})
		}

		w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)
		err = json.NewEncoder(w).Encode(result)
	default:
		err = WriteSnappyCompressed(w, readResult, logger)
	}

	if err != nil {
		h.promReadMetrics.incError(err)
	} else {
		h.promReadMetrics.fetchSuccess.Inc(1)
	}
}

type readResultsJSON struct {
	Queries []queryResultsJSON `json:"queries"`
}

type queryResultsJSON struct {
	Query  queryJSON           `json:"query"`
	Start  time.Time           `json:"start"`
	End    time.Time           `json:"end"`
	Series []comparator.Series `json:"series"`
}

type queryJSON struct {
	Matchers []labelMatcherJSON `json:"matchers"`
}

type labelMatcherJSON struct {
	Type  string `json:"type"`
	Name  string `json:"name"`
	Value string `json:"value"`
}

// WriteSnappyCompressed writes snappy compressed results to the given writer.
func WriteSnappyCompressed(
	w http.ResponseWriter,
	readResult ReadResult,
	logger *zap.Logger,
) error {
	resp := &prompb.ReadResponse{
		Results: readResult.Result,
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		logger.Error("unable to marshal read results to protobuf", zap.Error(err))
		xhttp.WriteError(w, err)
		return err
	}

	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		logger.Error("unable to encode read results to snappy",
			zap.Error(err))
		xhttp.WriteError(w, err)
	}

	return err
}

func parseCompressedRequest(
	r *http.Request,
) (*prompb.ReadRequest, error) {
	result, err := prometheus.ParsePromCompressedRequest(r)
	if err != nil {
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(result.UncompressedBody, &req); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return &req, nil
}

// ReadResult is a read result.
type ReadResult struct {
	Meta   block.ResultMetadata
	Result []*prompb.QueryResult
}

// ParseExpr parses a prometheus request expression into the constituent
// fetches, rather than the full query application.
func ParseExpr(
	r *http.Request,
	opts xpromql.ParseOptions,
) (*prompb.ReadRequest, error) {
	expr, err := parseExpr(r, opts)
	if err != nil {
		// Always invalid request if parsing fails params.
		return nil, xerrors.NewInvalidParamsError(err)
	}
	return expr, nil
}

func parseExpr(
	r *http.Request,
	opts xpromql.ParseOptions,
) (*prompb.ReadRequest, error) {
	var req *prompb.ReadRequest
	exprParam := strings.TrimSpace(r.FormValue("query"))
	if len(exprParam) == 0 {
		return nil, fmt.Errorf("cannot parse params: no expr")
	}

	queryStart, err := util.ParseTimeString(r.FormValue("start"))
	if err != nil {
		return nil, err
	}

	queryEnd, err := util.ParseTimeString(r.FormValue("end"))
	if err != nil {
		return nil, err
	}

	fn := opts.ParseFn()
	req = &prompb.ReadRequest{}
	expr, err := fn(exprParam)
	if err != nil {
		return nil, err
	}

	var vectorsInspected []*promql.VectorSelector
	promql.Inspect(expr, func(node promql.Node, path []promql.Node) error {
		var (
			start         = queryStart
			end           = queryEnd
			offset        time.Duration
			labelMatchers []*labels.Matcher
		)

		if n, ok := node.(*promql.MatrixSelector); ok {
			if n.Range > 0 {
				start = start.Add(-1 * n.Range)
			}

			vectorSelector := n.VectorSelector.(*promql.VectorSelector)

			// Check already inspected (matrix can be walked further into
			// child vector selector).
			for _, existing := range vectorsInspected {
				if existing == vectorSelector {
					return nil // Already inspected.
				}
			}

			vectorsInspected = append(vectorsInspected, vectorSelector)

			offset = vectorSelector.Offset
			labelMatchers = vectorSelector.LabelMatchers
		} else if n, ok := node.(*promql.VectorSelector); ok {
			// Check already inspected (matrix can be walked further into
			// child vector selector).
			for _, existing := range vectorsInspected {
				if existing == n {
					return nil // Already inspected.
				}
			}

			vectorsInspected = append(vectorsInspected, n)

			offset = n.Offset
			labelMatchers = n.LabelMatchers
		} else {
			return nil
		}

		if offset > 0 {
			start = start.Add(-1 * offset)
			end = end.Add(-1 * offset)
		}

		matchers, err := toLabelMatchers(labelMatchers)
		if err != nil {
			return err
		}

		query := &prompb.Query{
			StartTimestampMs: storage.TimeToPromTimestamp(start),
			EndTimestampMs:   storage.TimeToPromTimestamp(end),
			Matchers:         matchers,
		}

		req.Queries = append(req.Queries, query)
		return nil
	})

	return req, nil
}

// ParseRequest parses the compressed request
func ParseRequest(
	ctx context.Context,
	r *http.Request,
	opts options.HandlerOptions,
) (*prompb.ReadRequest, *storage.FetchOptions, error) {
	req, fetchOpts, err := parseRequest(ctx, r, opts)
	if err != nil {
		// Always invalid request if parsing fails params.
		return nil, nil, xerrors.NewInvalidParamsError(err)
	}
	return req, fetchOpts, nil
}

func parseRequest(
	ctx context.Context,
	r *http.Request,
	opts options.HandlerOptions,
) (*prompb.ReadRequest, *storage.FetchOptions, error) {
	var (
		req *prompb.ReadRequest
		err error
	)
	switch {
	case r.Method == http.MethodGet && strings.TrimSpace(r.FormValue("query")) != "":
		req, err = ParseExpr(r, opts.Engine().Options().ParseOptions())
	default:
		req, err = parseCompressedRequest(r)
	}
	if err != nil {
		return nil, nil, err
	}

	fetchOpts, rErr := opts.FetchOptionsBuilder().NewFetchOptions(r)
	if rErr != nil {
		return nil, nil, rErr
	}

	return req, fetchOpts, nil
}

// Read performs a remote read on the given engine.
func Read(
	ctx context.Context,
	cancelWatcher handler.CancelWatcher,
	r *prompb.ReadRequest,
	fetchOpts *storage.FetchOptions,
	opts options.HandlerOptions,
) (ReadResult, error) {
	var (
		queryCount   = len(r.Queries)
		cancelFuncs  = make([]context.CancelFunc, queryCount)
		queryResults = make([]*prompb.QueryResult, queryCount)
		meta         = block.NewResultMetadata()
		queryOpts    = &executor.QueryOptions{
			QueryContextOptions: models.QueryContextOptions{
				LimitMaxTimeseries: fetchOpts.SeriesLimit,
				LimitMaxDocs:       fetchOpts.DocsLimit,
			}}

		engine = opts.Engine()

		wg       sync.WaitGroup
		mu       sync.Mutex
		multiErr xerrors.MultiError
	)

	wg.Add(queryCount)
	for i, promQuery := range r.Queries {
		i, promQuery := i, promQuery // Capture vars for lambda.
		go func() {
			ctx, cancel := context.WithTimeout(ctx, fetchOpts.Timeout)
			defer func() {
				wg.Done()
				cancel()
			}()

			cancelFuncs[i] = cancel
			query, err := storage.PromReadQueryToM3(promQuery)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			// Detect clients closing connections.
			if cancelWatcher != nil {
				cancelWatcher.WatchForCancel(ctx, cancel)
			}

			result, err := engine.ExecuteProm(ctx, query, queryOpts, fetchOpts)
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
				return
			}

			result.PromResult.Timeseries = filterResults(
				result.PromResult.GetTimeseries(), fetchOpts)
			mu.Lock()
			queryResults[i] = result.PromResult
			meta = meta.CombineMetadata(result.Metadata)
			mu.Unlock()
		}()
	}

	wg.Wait()
	for _, cancel := range cancelFuncs {
		cancel()
	}

	if err := multiErr.FinalError(); err != nil {
		return ReadResult{Result: nil, Meta: meta}, err
	}

	return ReadResult{Result: queryResults, Meta: meta}, nil
}

// filterResults removes series tags based on options.
func filterResults(
	series []*prompb.TimeSeries,
	opts *storage.FetchOptions,
) []*prompb.TimeSeries {
	if opts == nil {
		return series
	}

	keys := opts.RestrictQueryOptions.GetRestrictByTag().GetFilterByNames()
	if len(keys) == 0 {
		return series
	}

	for i, s := range series {
		series[i].Labels = filterLabels(s.Labels, keys)
	}

	return series
}

func filterLabels(
	labels []prompb.Label,
	filtering [][]byte,
) []prompb.Label {
	if len(filtering) == 0 {
		return labels
	}

	filtered := labels[:0]
	for _, l := range labels {
		skip := false
		for _, f := range filtering {
			if bytes.Equal(l.GetName(), f) {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		filtered = append(filtered, l)
	}

	return filtered
}

func tagsConvert(ts models.Tags) comparator.Tags {
	tags := make(comparator.Tags, 0, ts.Len())
	for _, t := range ts.Tags {
		tags = append(tags, comparator.NewTag(string(t.Name), string(t.Value)))
	}

	return tags
}

func datapointsConvert(dps ts.Datapoints) comparator.Datapoints {
	datapoints := make(comparator.Datapoints, 0, dps.Len())
	for _, dp := range dps.Datapoints() {
		val := comparator.Datapoint{
			Value:     comparator.Value(dp.Value),
			Timestamp: dp.Timestamp,
		}
		datapoints = append(datapoints, val)
	}

	return datapoints
}

func toSeries(dps ts.Datapoints, tags models.Tags) comparator.Series {
	return comparator.Series{
		Tags:       tagsConvert(tags),
		Datapoints: datapointsConvert(dps),
	}
}

func toLabelMatchers(matchers []*labels.Matcher) ([]*prompb.LabelMatcher, error) {
	pbMatchers := make([]*prompb.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var mType prompb.LabelMatcher_Type
		switch m.Type {
		case labels.MatchEqual:
			mType = prompb.LabelMatcher_EQ
		case labels.MatchNotEqual:
			mType = prompb.LabelMatcher_NEQ
		case labels.MatchRegexp:
			mType = prompb.LabelMatcher_RE
		case labels.MatchNotRegexp:
			mType = prompb.LabelMatcher_NRE
		default:
			return nil, errors.New("invalid matcher type")
		}
		pbMatchers = append(pbMatchers, &prompb.LabelMatcher{
			Type:  mType,
			Name:  []byte(m.Name),
			Value: []byte(m.Value),
		})
	}
	return pbMatchers, nil
}
