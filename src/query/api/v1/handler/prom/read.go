// Package prom provides custom handlers that support the prometheus
// query endpoints.
package prom

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type readHandler struct {
	engine    *promql.Engine
	queryable promstorage.Queryable
	hOpts     options.HandlerOptions
	scope     tally.Scope
	logger    *zap.Logger
}

type readRequest struct {
	query         string
	start, end    time.Time
	step, timeout time.Duration
}

func newReadHandler(
	opts Options,
	hOpts options.HandlerOptions,
	queryable promstorage.Queryable,
) http.Handler {
	scope := hOpts.InstrumentOpts().MetricsScope().Tagged(
		map[string]string{"handler": "prometheus-read"},
	)
	return &readHandler{
		engine:    opts.PromQLEngine,
		queryable: queryable,
		hOpts:     hOpts,
		scope:     scope,
		logger:    hOpts.InstrumentOpts().Logger(),
	}
}

func (h *readHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	request, err := parseRequest(r)
	if err != nil {
		respondError(w, err, http.StatusBadRequest)
		return
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
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	if request.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.timeout)
		defer cancel()
	}

	qry, err := h.engine.NewRangeQuery(
		h.queryable,
		request.query,
		request.start,
		request.end,
		request.step)
	if err != nil {
		h.logger.Error("error creating range query", zap.Error(err), zap.String("query", request.query))
		respondError(w, err, http.StatusInternalServerError)
		return
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if res.Err != nil {
		h.logger.Error("error executing range query", zap.Error(res.Err), zap.String("query", request.query))
		respondError(w, res.Err, http.StatusInternalServerError)
		return
	}

	respond(w, &queryData{
		Result:     res.Value,
		ResultType: res.Value.Type(),
	}, res.Warnings)
}

// The below methods are taken from prometheus.
// https://github.com/prometheus/prometheus/blob/43acd0e2e93f9f70c49b2267efa0124f1e759e86/web/api/v1/api.go#L319.

func parseRequest(r *http.Request) (readRequest, error) {
	var (
		params readRequest
		err    error
	)

	params.start, err = parseTime(r.FormValue(startParam))
	if err != nil {
		return readRequest{}, fmt.Errorf("invalid parameter 'start': %w", err)
	}

	params.end, err = parseTime(r.FormValue(endParam))
	if err != nil {
		return params, fmt.Errorf("invalid parameter 'end': %w", err)
	}

	params.step, err = parseDuration(r.FormValue("step"))
	if err != nil {
		return params, fmt.Errorf("invalid parameter 'step': %w", err)
	}

	if params.step <= 0 {
		return params, errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
	}

	if to := r.FormValue("timeout"); to != "" {
		params.timeout, err = parseDuration(to)
		if err != nil {
			return params, fmt.Errorf("invalid parameter 'timeout': %w", err)
		}
	}

	params.query = r.FormValue(queryParam)

	return params, nil
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
