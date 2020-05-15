package prom

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/prometheus"
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
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	if t := r.FormValue("timeout"); t != "" {
		timeout, err := parseDuration(t)
		if err != nil {
			err = fmt.Errorf("invalid parameter 'timeout': %w", err)
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

	respond(w, &queryData{
		Result:     res.Value,
		ResultType: res.Value.Type(),
	}, res.Warnings)
}
