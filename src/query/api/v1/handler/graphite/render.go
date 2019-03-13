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

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/native"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/x/net/http"
)

const (
	// ReadURL is the url for the graphite query handler.
	ReadURL = handler.RoutePrefixV1 + "/graphite/render"
)

var (
	// ReadHTTPMethods is the HTTP methods used with this resource.
	ReadHTTPMethods = []string{http.MethodGet, http.MethodPost}
)

// A renderHandler implements the graphite /render endpoint, including full
// support for executing functions. It only works against data in M3.
type renderHandler struct {
	engine        *native.Engine
	clusterLimits limits
	limitCfg      *config.LimitsConfiguration
}

type respError struct {
	err  error
	code int
}

type limit struct {
	retention  time.Duration
	resolution time.Duration
}

type limits []limit

func (ls limits) Len() int           { return len(ls) }
func (ls limits) Swap(i, j int)      { ls[i], ls[j] = ls[j], ls[i] }
func (ls limits) Less(i, j int) bool { return ls[i].retention < ls[j].retention }

func (ls limits) validateLimits(
	start time.Time,
	end time.Time,
	limits *config.LimitsConfiguration,
) error {
	// no limits configured; skip validation.
	if limits == nil || limits.MaxComputedDatapoints == 0 {
		return nil
	}

	if len(ls) == 0 {
		return errors.New("no aggregated namespaces configured")
	}

	longestPeriod := time.Since(start)

	var (
		shortestRes time.Duration
		found       bool
	)

	period := end.Sub(start)
	for _, l := range ls {
		if l.retention > longestPeriod {
			res := l.resolution
			if !found {
				found = true
				shortestRes = res
			} else if res < shortestRes {
				shortestRes = res
			}
		}
	}

	// if no cluster has a long enough retention, take the resolution of the
	// longest available cluster to estimate limits.
	if !found || shortestRes <= 0 {
		shortestRes = ls[len(ls)-1].resolution
	}

	// NB: this is a sanity check, as ot should never hit here; this is the case
	// where a cluster has a 0 or negative resolution
	if shortestRes <= 0 {
		return fmt.Errorf("detected resolution is invalid: %d should be greater "+
			"than 0", shortestRes)
	}

	approximateDatapointCount := int64(period / shortestRes)
	if approximateDatapointCount > limits.MaxComputedDatapoints {
		return fmt.Errorf("expected datapoint count %d is higher than maximum "+
			"limit: %d", approximateDatapointCount, limits.MaxComputedDatapoints)
	}

	return nil
}

// NB: it's difficult to determine the datapoint count pre-emptively with great
// accuracy here since it's hard to know if the particular cluster will be used
// to satisfy the query. Can do a rough guess by finding the cluster which has
// the shortest resolution while having a retention that covers the query range.
//
// Keep in mind that this is a rough guess at upper bound range and likely far
// too permissive.
func generateApproximateLimits(clusters m3.Clusters) limits {
	namespaces := clusters.ClusterNamespaces()
	limits := make(limits, 0, len(namespaces))
	i := 0
	for _, ns := range namespaces {
		attrs := ns.Options().Attributes()
		if attrs.MetricsType != storage.AggregatedMetricsType {
			continue
		}

		i++
		limits = append(limits, limit{
			retention:  attrs.Retention,
			resolution: attrs.Resolution,
		})
	}

	limits = limits[:i]
	sort.Sort(limits)
	return limits
}

// NewRenderHandler returns a new render handler around the given storage.
func NewRenderHandler(
	storage storage.Storage,
	clusters m3.Clusters,
	limitCfg *config.LimitsConfiguration,
) http.Handler {
	wrappedStore := graphite.NewM3WrappedStorage(storage)
	var (
		clusterLimits limits
	)
	if clusters != nil {
		clusterLimits = generateApproximateLimits(clusters)
	} else {
		clusterLimits = limits{}
	}

	return &renderHandler{
		engine:        native.NewEngine(wrappedStore),
		clusterLimits: clusterLimits,
		limitCfg:      limitCfg,
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
	respErr := h.serveHTTP(w, r)
	if respErr.err != nil {
		xhttp.Error(w, respErr.err, respErr.code)
	}
}

func (h *renderHandler) serveHTTP(
	w http.ResponseWriter,
	r *http.Request,
) respError {
	reqCtx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	p, err := ParseRenderRequest(r)
	if err != nil {
		return respError{err: err, code: http.StatusBadRequest}
	}

	err = h.clusterLimits.validateLimits(p.From, p.Until, h.limitCfg)
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
				)
				targetSeries.Values[i] = ts.LTTB(s, s.StartTime(), s.EndTime(), newMillisPerStep)
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

	err = WriteRenderResponse(w, response, p.Format)
	return respError{err: err, code: http.StatusOK}
}
