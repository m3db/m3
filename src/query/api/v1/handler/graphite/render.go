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
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/native"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/net/http"
)

const (
	// GraphiteReadURL is the url for native graphite query handler.
	GraphiteReadURL = handler.RoutePrefixV1 + "/graphite/render"
)

var (
	// GraphiteReadHTTPMethods is the HTTP methods used with this resource.
	GraphiteReadHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

// A nativeRenderHandler implements the graphite /render endpoint, including full
// support for executing functions. It only works against data in M3.
type nativeRenderHandler struct {
	engine *native.Engine
}

type respError struct {
	err  error
	code int
}

// NewNativeRenderHandler returns a new native render handler around the given engine
func NewNativeRenderHandler(
	storage storage.Storage,
) http.Handler {
	wrappedStore := graphite.NewM3WrappedStorage(storage)
	return &nativeRenderHandler{
		engine: native.NewEngine(wrappedStore),
	}
}

func sendError(errorCh chan error, err error) {
	select {
	case errorCh <- err:
	default:
	}
}

// ServeHTTP processes the render requests.
func (h *nativeRenderHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	respErr := h.serveHTTP(w, r)
	if respErr.err != nil {
		xhttp.Error(w, respErr.err, respErr.code)
	}
}

func (h *nativeRenderHandler) serveHTTP(
	w http.ResponseWriter,
	r *http.Request,
) respError {
	reqCtx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	p, err := ParseRenderRequest(r)
	if err != nil {
		return respError{err: err, code: http.StatusBadRequest}
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
	})

	// Set the request context.
	ctx.SetRequestContext(reqCtx)
	defer ctx.Close()

	var wg sync.WaitGroup
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
				childCtx.Close()
				wg.Done()
			}()

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
					newStepDuration    = time.Duration(newMillisPerStep) * time.Millisecond
					consolidationFunc  = s.ConsolidationFunc()
					shiftedStart       = s.StartTime().Truncate(newStepDuration)
				)

				if shiftedStart.Before(s.StartTime()) {
					shiftedStart = shiftedStart.Add(newStepDuration)
				}

				// TODO: consider LTTB approach here depending on query options
				series, err := s.IntersectAndResize(shiftedStart, s.EndTime().Add(-time.Nanosecond),
					newMillisPerStep, consolidationFunc)
				if err != nil {
					sendError(errorCh, errors.NewRenamedError(err,
						fmt.Errorf("error: unable to resize series returned %s", err)))
					return
				}

				targetSeries.Values[i] = series
			}

			mu.Lock()
			results[i] = targetSeries
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(errorCh)
	err = <-errorCh
	if err != nil {
		return respError{err: err, code: http.StatusInternalServerError}
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

	err = WriteRenderResponse(w, response)
	return respError{err: err, code: http.StatusOK}
}
