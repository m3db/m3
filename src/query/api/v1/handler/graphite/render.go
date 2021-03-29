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
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/native"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// ReadURL is the url for the graphite query handler.
	ReadURL = handler.RoutePrefixV1 + "/graphite/render"
)

var (
	// ReadHTTPMethods are the HTTP methods used with this resource.
	ReadHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

// A renderHandler implements the graphite /render endpoint, including full
// support for executing functions. It only works against data in M3.
type renderHandler struct {
	opts             options.HandlerOptions
	engine           *native.Engine
	queryContextOpts models.QueryContextOptions
	graphiteOpts     graphite.M3WrappedStorageOptions
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
		}),
		queryContextOpts: opts.QueryContextOptions(),
		graphiteOpts:     opts.GraphiteStorageOptions(),
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
		xhttp.WriteError(w, err)
	}
}

func (h *renderHandler) serveHTTP(
	w http.ResponseWriter,
	r *http.Request,
) error {
	reqCtx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	p, fetchOpts, err := ParseRenderRequest(r, h.opts)
	if err != nil {
		return xhttp.NewError(err, http.StatusBadRequest)
	}

	limit, err := handleroptions.ParseLimit(r, headers.LimitMaxSeriesHeader,
		"limit", h.queryContextOpts.LimitMaxTimeseries)
	if err != nil {
		return xhttp.NewError(err, http.StatusBadRequest)
	}

	var (
		results = make([]ts.SeriesList, len(p.Targets))
		errorCh = make(chan error, 1)
		mu      sync.Mutex
	)

	ctx := common.NewContext(common.ContextOptions{
		Engine:  h.engine,
		Start:   p.From,
		End:     p.Until,
		Timeout: p.Timeout,
		Limit:   limit,
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
			// Log the query that causes us to panic.
			defer func() {
				if err := recover(); err != nil {
					panic(fmt.Errorf("panic executing query '%s': %v", target, err))
				}
			}()

			childCtx := ctx.NewChildContext(common.NewChildContextOptions())
			defer func() {
				_ = childCtx.Close()
				wg.Done()
			}()

			if source := r.Header.Get(headers.SourceHeader); len(source) > 0 {
				childCtx.Source = []byte(source)
			}

			exp, err := h.engine.Compile(target)
			if err != nil {
				sendError(errorCh, errors.NewRenamedError(err,
					fmt.Errorf("invalid 'target': %s => %s", target, err)))
				return
			}

			targetSeries, err := exp.Execute(childCtx)
			if err != nil {
				sendError(errorCh, errors.NewRenamedError(err,
					fmt.Errorf("error: target %s returned %s", target, err)))
				return
			}

			for i, s := range targetSeries.Values {
				if s.Len() <= int(p.MaxDataPoints) {
					continue
				}

				var (
					samplingMultiplier = math.Ceil(float64(s.Len()) / float64(p.MaxDataPoints))
					newMillisPerStep   = int(samplingMultiplier * float64(s.MillisPerStep()))
				)
				targetSeries.Values[i] = ts.LTTB(s, s.StartTime(), s.EndTime(), newMillisPerStep)
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

	handleroptions.AddResponseHeaders(w, meta, fetchOpts)

	return WriteRenderResponse(w, response, p.Format, renderResultsJSONOptions{
		renderSeriesAllNaNs: h.graphiteOpts.RenderSeriesAllNaNs,
	})
}
