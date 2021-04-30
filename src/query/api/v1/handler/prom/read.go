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
	queryerrors "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/prometheus"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/prometheus/prometheus/promql"
	promstorage "github.com/prometheus/prometheus/storage"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// NewQueryFn creates a new promql Query.
type NewQueryFn func(params models.RequestParams) (promql.Query, error)

var (
	newRangeQueryFn = func(
		engine *promql.Engine,
		queryable promstorage.Queryable,
	) NewQueryFn {
		return func(params models.RequestParams) (promql.Query, error) {
			return engine.NewRangeQuery(
				queryable,
				params.Query,
				params.Start,
				params.End,
				params.Step)
		}
	}

	newInstantQueryFn = func(
		engine *promql.Engine,
		queryable promstorage.Queryable,
	) NewQueryFn {
		return func(params models.RequestParams) (promql.Query, error) {
			return engine.NewInstantQuery(
				queryable,
				params.Query,
				params.Now)
		}
	}
)

type readHandler struct {
	hOpts               options.HandlerOptions
	scope               tally.Scope
	logger              *zap.Logger
	opts                opts
	returnedDataMetrics native.PromReadReturnedDataMetrics
}

func newReadHandler(
	hOpts options.HandlerOptions,
	options opts,
) (http.Handler, error) {
	scope := hOpts.InstrumentOpts().MetricsScope().Tagged(
		map[string]string{"handler": "prometheus-read"},
	)
	return &readHandler{
		hOpts:               hOpts,
		opts:                options,
		scope:               scope,
		logger:              hOpts.InstrumentOpts().Logger(),
		returnedDataMetrics: native.NewPromReadReturnedDataMetrics(scope),
	}, nil
}

func (h *readHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, request, err := native.ParseRequest(ctx, r, h.opts.instant, h.hOpts)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	params := request.Params
	fetchOptions := request.FetchOpts

	// NB (@shreyas): We put the FetchOptions in context so it can be
	// retrieved in the queryable object as there is no other way to pass
	// that through.
	var resultMetadata block.ResultMetadata
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	ctx = context.WithValue(ctx, prometheus.BlockResultMetadataKey, &resultMetadata)

	qry, err := h.opts.newQueryFn(params)
	if err != nil {
		h.logger.Error("error creating query",
			zap.Error(err), zap.String("query", params.Query),
			zap.Bool("instant", h.opts.instant))
		xhttp.WriteError(w, xerrors.NewInvalidParamsError(err))
		return
	}
	defer qry.Close()

	res := qry.Exec(ctx)
	if res.Err != nil {
		h.logger.Error("error executing query",
			zap.Error(res.Err), zap.String("query", params.Query),
			zap.Bool("instant", h.opts.instant))
		var sErr *prometheus.StorageErr
		if errors.As(res.Err, &sErr) {
			// If the error happened in the m3 storage layer, propagate the causing error as is.
			err := sErr.Unwrap()
			if queryerrors.IsTimeout(err) {
				xhttp.WriteError(w, queryerrors.NewErrQueryTimeout(err))
			} else {
				xhttp.WriteError(w, err)
			}
		} else {
			promErr := res.Err
			switch promErr.(type) { //nolint:errorlint
			case promql.ErrQueryTimeout:
				promErr = queryerrors.NewErrQueryTimeout(promErr)
			case promql.ErrQueryCanceled:
			default:
				// Assume any prometheus library error is a 4xx, since there are no remote calls.
				promErr = xerrors.NewInvalidParamsError(res.Err)
			}
			xhttp.WriteError(w, promErr)
		}
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

	err = handleroptions.AddDBLimitResponseHeaders(w, resultMetadata, fetchOptions)
	if err != nil {
		h.logger.Error("error writing db limit response headers",
			zap.Error(err), zap.String("query", query),
			zap.Bool("instant", h.opts.instant))
		xhttp.WriteError(w, err)
		return
	}

	// TODO(nate): make execution of this configurable
	returnedDataLimited, err := LimitReturnedData(res, fetchOptions)
	if err != nil {
		h.logger.Error("error enforcing returned data limits",
			zap.Error(err), zap.String("query", query),
			zap.Bool("instant", h.opts.instant))
		xhttp.WriteError(w, err)
		return
	}
	h.returnedDataMetrics.FetchDatapoints.RecordValue(float64(returnedDataLimited.Datapoints))
	h.returnedDataMetrics.FetchSeries.RecordValue(float64(returnedDataLimited.Series))

	limited := &handleroptions.ReturnedDataLimited{
		Limited:     returnedDataLimited.Limited,
		Series:      returnedDataLimited.Series,
		TotalSeries: returnedDataLimited.TotalSeries,
		Datapoints:  returnedDataLimited.Datapoints,
	}
	err = handleroptions.AddReturnedLimitResponseHeaders(w, limited, nil)
	if err != nil {
		h.logger.Error("error writing response headers",
			zap.Error(err), zap.String("query", query),
			zap.Bool("instant", h.opts.instant))
		xhttp.WriteError(w, err)
		return
	}

	err = Respond(w, &QueryData{
		Result:     res.Value,
		ResultType: res.Value.Type(),
	}, res.Warnings)
	if err != nil {
		h.logger.Error("error writing prom response",
			zap.Error(res.Err), zap.String("query", params.Query),
			zap.Bool("instant", h.opts.instant))
	}
}
