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
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage/prometheus"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type readInstantHandler struct {
	queryable promstorage.Queryable
	engine    *promql.Engine
	hOpts     options.HandlerOptions
	scope     tally.Scope
	logger    *zap.Logger
}

func newReadInstantHandler(
	opts Options,
	hOpts options.HandlerOptions,
	queryable promstorage.Queryable,
) http.Handler {
	scope := hOpts.InstrumentOpts().MetricsScope().Tagged(
		map[string]string{"handler": "prometheus-read-instantaneous"},
	)
	return &readInstantHandler{
		engine:    opts.PromQLEngine,
		queryable: queryable,
		hOpts:     hOpts,
		scope:     scope,
		logger:    hOpts.InstrumentOpts().Logger(),
	}
}

func (h *readInstantHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var ts time.Time
	if t := r.FormValue("time"); t != "" {
		var err error
		ts, err = parseTime(t)
		if err != nil {
			respondError(w, err, http.StatusBadRequest)
			return
		}
	} else {
		ts = time.Now()
	}

	fetchOptions, fetchErr := h.hOpts.FetchOptionsBuilder().NewFetchOptions(r)
	if fetchErr != nil {
		respondError(w, fetchErr, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	// NB (@shreyas): We put the FetchOptions in context so it can be
	// retrieved in the queryable object as there is no other way to pass
	// that through.
	var resultMetadata block.ResultMetadata
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	ctx = context.WithValue(ctx, prometheus.BlockResultMetadataKey, &resultMetadata)

	if t := r.FormValue("timeout"); t != "" {
		timeout, err := parseDuration(t)
		if err != nil {
			err = fmt.Errorf("invalid parameter 'timeout': %v", err)
			respondError(w, err, http.StatusBadRequest)
			return
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	query := r.FormValue("query")
	qry, err := h.engine.NewInstantQuery(
		h.queryable,
		query,
		ts)
	if err != nil {
		h.logger.Error("error creating instant query", zap.Error(err), zap.String("query", query))
		respondError(w, err, http.StatusInternalServerError)
		return
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if res.Err != nil {
		h.logger.Error("error executing instant query", zap.Error(res.Err), zap.String("query", query))
		respondError(w, res.Err, http.StatusInternalServerError)
		return
	}

	handleroptions.AddWarningHeaders(w, resultMetadata)

	respond(w, &queryData{
		Result:     res.Value,
		ResultType: res.Value.Type(),
	}, res.Warnings)
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return minTime, nil
	case maxTimeFormatted:
		return maxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
