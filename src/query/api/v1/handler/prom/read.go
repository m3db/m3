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

// Package prom provides custom handlers that support the prometheus
// query endpoints.
package prom

import (
	"context"
	"errors"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/prometheus"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// ReadHandlerHooks allows dynamic plugging into read request processing.
type ReadHandlerHooks interface {
	// OnParsedRequest gets invoked after parsing request arguments.
	OnParsedRequest(
		context.Context,
		*http.Request,
		models.RequestParams,
	) (models.RequestParams, error)
}

// NewQueryFn creates a new promql Query.
type NewQueryFn func(
	engine *promql.Engine,
	queryable promstorage.Queryable,
	params models.RequestParams,
) (promql.Query, error)

var (
	newRangeQueryFn = func(
		engine *promql.Engine,
		queryable promstorage.Queryable,
		params models.RequestParams,
	) (promql.Query, error) {
		return engine.NewRangeQuery(
			queryable,
			params.Query,
			params.Start,
			params.End,
			params.Step)
	}

	newInstantQueryFn = func(
		engine *promql.Engine,
		queryable promstorage.Queryable,
		params models.RequestParams,
	) (promql.Query, error) {
		return engine.NewInstantQuery(
			queryable,
			params.Query,
			params.Now)
	}
)

type readHandler struct {
	engine     *promql.Engine
	hooks      ReadHandlerHooks
	queryable  promstorage.Queryable
	newQueryFn NewQueryFn
	hOpts      options.HandlerOptions
	scope      tally.Scope
	logger     *zap.Logger
	opts       Options
}

func newReadHandler(
	opts Options,
	hOpts options.HandlerOptions,
	hooks ReadHandlerHooks,
	queryable promstorage.Queryable,
) http.Handler {
	scope := hOpts.InstrumentOpts().MetricsScope().Tagged(
		map[string]string{"handler": "prometheus-read"},
	)

	newQueryFn := newRangeQueryFn
	if opts.instant {
		newQueryFn = newInstantQueryFn
	}

	return &readHandler{
		engine:     opts.PromQLEngine,
		hooks:      hooks,
		queryable:  queryable,
		newQueryFn: newQueryFn,
		hOpts:      hOpts,
		opts:       opts,
		scope:      scope,
		logger:     hOpts.InstrumentOpts().Logger(),
	}
}

func (h *readHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	fetchOptions, err := h.hOpts.FetchOptionsBuilder().NewFetchOptions(r)
	if err != nil {
		RespondError(w, err)
		return
	}

	request, err := native.ParseRequest(ctx, r, h.opts.instant, h.hOpts)
	if err != nil {
		RespondError(w, err)
		return
	}

	params, err := h.hooks.OnParsedRequest(ctx, r, request.Params)
	if err != nil {
		RespondError(w, err)
		return
	}

	// NB (@shreyas): We put the FetchOptions in context so it can be
	// retrieved in the queryable object as there is no other way to pass
	// that through.
	var resultMetadata block.ResultMetadata
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	ctx = context.WithValue(ctx, prometheus.BlockResultMetadataKey, &resultMetadata)

	if params.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, params.Timeout)
		defer cancel()
	}

	qry, err := h.newQueryFn(h.engine, h.queryable, params)
	if err != nil {
		h.logger.Error("error creating query",
			zap.Error(err), zap.String("query", params.Query),
			zap.Bool("instant", h.opts.instant))
		RespondError(w, xerrors.NewInvalidParamsError(err))
		return
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if res.Err != nil {
		h.logger.Error("error executing query",
			zap.Error(res.Err), zap.String("query", params.Query),
			zap.Bool("instant", h.opts.instant))
		RespondError(w, res.Err)
		return
	}

	for _, warn := range resultMetadata.Warnings {
		res.Warnings = append(res.Warnings, errors.New(warn.Message))
	}

	query := params.Query
	err = ApplyRangeWarnings(query, &resultMetadata)
	if err != nil {
		h.logger.Warn("error applying range warnings",
			zap.Error(err), zap.String("query", query),
			zap.Bool("instant", h.opts.instant))
	}

	handleroptions.AddWarningHeaders(w, resultMetadata)
	Respond(w, &QueryData{
		Result:     res.Value,
		ResultType: res.Value.Type(),
	}, res.Warnings)
}

type noopReadHandlerHooks struct{}

func (h *noopReadHandlerHooks) OnParsedRequest(
	_ context.Context,
	_ *http.Request,
	params models.RequestParams,
) (models.RequestParams, error) {
	return params, nil
}
