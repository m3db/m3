// Copyright (c) 2019 Uber Technologies, Inc.
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

package graphite

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/block"
	queryerrors "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/native"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	// ReadURL is the url for the graphite query handler.
	ReadURL = route.Prefix + "/graphite/render"

	logHighInterval = 30 * time.Second
)

// ReadHTTPMethods are the HTTP methods used with this resource.
var ReadHTTPMethods = []string{http.MethodGet, http.MethodPost}

// A renderHandler implements the graphite /render endpoint, including full
// support for executing functions. It only works against data in M3.
type renderHandler struct {
	opts                options.HandlerOptions
	engine              *native.Engine
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	queryContextOpts    models.QueryContextOptions
	graphiteOpts        graphite.M3WrappedStorageOptions
	instrumentOpts      instrument.Options

	logStatsMutex              sync.Mutex
	logStats                   []renderStats
	lastLoggedAlignedUnixNanos int64

	metrics renderHandlerMetrics
	logger  *zap.Logger
}

type renderStats struct {
	req   RenderRequest
	stats common.QueryStats
}

type renderHandlerMetrics struct {
	queryFetchesTotal        tally.Counter
	queryFetchesHistogram    tally.Histogram
	queryShiftsConditional   tally.Counter
	queryShiftsUnconditional tally.Counter
	queryShiftsHistogram     tally.Histogram
}

func newRenderHandlerMetrics(scope tally.Scope) renderHandlerMetrics {
	return renderHandlerMetrics{
		queryFetchesTotal: scope.Counter("query-fetch-expressions-total"),
		queryFetchesHistogram: scope.Histogram("query-fetches", tally.ValueBuckets{
			0, 1, 2, 4, 6, 8, 10, 12, 14, 16, 24, 32, 48, 64, 128, 256, 512,
		}),
		queryShiftsConditional:   scope.Counter("query-shifts-conditional"),
		queryShiftsUnconditional: scope.Counter("query-shifts-unconditional"),
		queryShiftsHistogram: scope.Histogram("query-shifts-total-distribution", tally.ValueBuckets{
			0, 1, 2, 4, 6, 8, 10, 12, 14, 16, 24, 32, 48, 64, 128, 256, 512,
		}),
	}
}

type respError struct {
	err  error
	code int
}

// NewRenderHandler returns a new render handler around the given storage.
func NewRenderHandler(opts options.HandlerOptions) http.Handler {
	wrappedStore := graphite.NewM3WrappedStorage(opts.Storage(),
		opts.M3DBOptions(), opts.InstrumentOpts(), opts.GraphiteStorageOptions())
	return &renderHandler{
		opts: opts,
		engine: native.NewEngine(wrappedStore, native.CompileOptions{
			EscapeAllNotOnlyQuotes: opts.GraphiteStorageOptions().CompileEscapeAllNotOnlyQuotes,
			InstrumentOptions:      opts.InstrumentOpts(),
		}),
		fetchOptionsBuilder: opts.GraphiteRenderFetchOptionsBuilder(),
		queryContextOpts:    opts.QueryContextOptions(),
		graphiteOpts:        opts.GraphiteStorageOptions(),
		instrumentOpts:      opts.InstrumentOpts(),
		metrics:             newRenderHandlerMetrics(opts.InstrumentOpts().MetricsScope()),
		logger:              opts.InstrumentOpts().Logger(),
	}
}

func sendError(errorCh chan error, err error) {
	select {
	case errorCh <- err:
	default:
	}
}

// ServeHTTP processes the render requests.
func (h *renderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.serveHTTP(w, r); err != nil {
		if queryerrors.IsTimeout(err) {
			err = queryerrors.NewErrQueryTimeout(err)
		}
		xhttp.WriteError(w, err)
	}
}

func (h *renderHandler) serveHTTP(
	w http.ResponseWriter,
	r *http.Request,
) error {
	reqCtx, p, fetchOpts, err := ParseRenderRequest(r.Context(), r, h.fetchOptionsBuilder)
	if err != nil {
		return xhttp.NewError(err, http.StatusBadRequest)
	}

	var (
		results = make([]ts.SeriesList, len(p.Targets))
		errorCh = make(chan error, 1)
		mu      sync.Mutex
	)

	ctx := common.NewContext(common.ContextOptions{
		Engine:                      h.engine,
		Start:                       p.From,
		End:                         p.Until,
		Timeout:                     p.Timeout,
		MaxDataPoints:               p.MaxDataPoints,
		MaxSubExpressionEvaluations: int64(h.graphiteOpts.MaxSubExpressionEvaluations),
		FetchOpts:                   fetchOpts,
	})

	// Set the request context.
	ctx.SetRequestContext(reqCtx)
	defer ctx.Close()

	var wg sync.WaitGroup
	meta := block.NewResultMetadata()
	wg.Add(len(p.Targets))
	for i, target := range p.Targets {
		i, target := i, target
		go func() {
			var childCtx *common.Context
			defer func() {
				if err := recover(); err != nil {
					// Allow recover from panic.
					sendError(errorCh, fmt.Errorf("error target '%s' caused panic: %v", target, err))

					// Log panic.
					logger := logging.WithContext(r.Context(), h.instrumentOpts).
						WithOptions(zap.AddStacktrace(zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
							return lvl >= zapcore.ErrorLevel
						})))
					logger.Error("panic captured", zap.Any("stack", err))
				}
				if childCtx != nil {
					_ = childCtx.Close()
				}
				wg.Done()
			}()

			exp, err := h.engine.Compile(target)
			if err != nil {
				sendError(errorCh, errors.NewRenamedError(err,
					fmt.Errorf("invalid 'target': %s => %s", target, err)))
				return
			}

			childCtxOpts := common.NewChildContextOptionsWithQuery(target, exp)
			childCtx = ctx.NewChildContext(childCtxOpts)

			targetSeries, err := exp.Execute(childCtx)
			if err != nil {
				sendError(errorCh, errors.NewRenamedError(err,
					fmt.Errorf("error target '%s' returned: %w", target, err)))
				return
			}

			// Apply LTTB downsampling to any series that hasn't been resized
			// to fit max datapoints explicitly using "consolidateBy" function.
			for i, s := range targetSeries.Values {
				resizeMillisPerStep, needResize := s.ResizeToMaxDataPointsMillisPerStep(p.MaxDataPoints)
				if !needResize {
					continue
				}

				targetSeries.Values[i] = ts.LTTB(s, s.StartTime(), s.EndTime(), resizeMillisPerStep)
			}

			mu.Lock()
			meta = meta.CombineMetadata(targetSeries.Metadata)
			results[i] = targetSeries
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(errorCh)
	err = <-errorCh
	if err != nil {
		return err
	}

	// Count and sort the groups if not sorted already.
	// NB(r): For certain things like stacking different targets in Grafana
	// returning targets in order matters to give a deterministic order for
	// the series to display when stacking. However we should only mutate
	// the order if no expressions have explicitly applied their own sort.
	numSeries := 0
	for _, r := range results {
		numSeries += r.Len()
		if !r.SortApplied {
			// Use sort.Stable for deterministic output.
			sort.Stable(ts.SeriesByName(r.Values))
		}
	}

	series := make([]*ts.Series, 0, numSeries)
	for _, r := range results {
		series = append(series, r.Values...)
	}

	// We've always sorted the response by this point
	response := ts.SeriesList{
		Values:      series,
		SortApplied: true,
	}

	if err := handleroptions.AddDBResultResponseHeaders(w, meta, fetchOpts); err != nil {
		return err
	}

	stats := ctx.QueryStats()
	h.collectRenderStats(p, stats)
	h.metrics.queryFetchesTotal.Inc(stats.Fetch.Total)
	h.metrics.queryFetchesHistogram.RecordValue(float64(stats.Fetch.Total))
	h.metrics.queryShiftsHistogram.RecordValue(float64(stats.TimeRangeAdjustment.Total))
	h.metrics.queryShiftsConditional.Inc(stats.TimeRangeAdjustment.ConditionalAdjustments)
	h.metrics.queryShiftsUnconditional.Inc(stats.TimeRangeAdjustment.UnconditionalAdjustments)

	return WriteRenderResponse(w, response, p.Format, renderResultsJSONOptions{
		renderSeriesAllNaNs: h.graphiteOpts.RenderSeriesAllNaNs,
	})
}

func (h *renderHandler) collectRenderStats(
	p RenderRequest,
	stats common.QueryStats,
) {
	h.logStatsMutex.Lock()
	h.logStats = append(h.logStats, renderStats{req: p, stats: stats})
	h.logStatsMutex.Unlock()

	last := atomic.LoadInt64(&h.lastLoggedAlignedUnixNanos)
	now := time.Now().Truncate(logHighInterval).UnixNano()
	if last == now {
		return
	}
	if !atomic.CompareAndSwapInt64(&h.lastLoggedAlignedUnixNanos, last, now) {
		return
	}

	h.logStatsMutex.Lock()
	sort.Slice(h.logStats, func(i, j int) bool {
		return h.logStats[i].stats.Fetch.Total < h.logStats[j].stats.Fetch.Total
	})
	// Get highest after sorting.
	highest := h.logStats[len(h.logStats)-1]
	// Reset.
	for i := range h.logStats {
		h.logStats[i] = renderStats{}
	}
	h.logStats = h.logStats[:0]
	// Unlock and emit stats.
	h.logStatsMutex.Unlock()

	h.logger.Warn(
		fmt.Sprintf("query with highest fetches in last %s", logHighInterval.String()),
		zap.Strings("query", highest.req.Targets),
		zap.Int64("fetches", highest.stats.Fetch.Total),
		zap.Int64("shifts", highest.stats.TimeRangeAdjustment.Total),
		zap.Int64("unconditionalShifts", stats.TimeRangeAdjustment.UnconditionalAdjustments),
		zap.Int64("conditionalShifts", stats.TimeRangeAdjustment.ConditionalAdjustments))
}
