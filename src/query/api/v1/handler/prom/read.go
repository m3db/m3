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
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/prometheus"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
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
		xhttp.WriteError(w, res.Err)
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

	limited, data := h.limitReturnedData(query, res, fetchOptions)

	if limited {
		if err := native.WriteReturnedDataLimitedHeader(w, data); err != nil {
			h.logger.Error("error writing returned data limited header",
				zap.Error(err), zap.String("query", query),
				zap.Bool("instant", h.opts.instant))
		}
	}

	h.returnedDataMetrics.FetchDatapoints.RecordValue(float64(data.Datapoints))
	h.returnedDataMetrics.FetchSeries.RecordValue(float64(data.Series))

	handleroptions.AddResponseHeaders(w, resultMetadata, fetchOptions)
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

func (h *readHandler) limitReturnedData(query string,
	res *promql.Result,
	fetchOpts *storage.FetchOptions,
) (bool, native.ReturnedDataLimited) {
	var (
		seriesLimit     = fetchOpts.ReturnedSeriesLimit
		datapointsLimit = fetchOpts.ReturnedDatapointsLimit

		limited     = false
		series      int
		datapoints  int
		seriesTotal int
	)
	switch res.Value.Type() {
	case parser.ValueTypeVector:
		v, err := res.Vector()
		if err != nil {
			h.logger.Error("error parsing vector for returned data limits",
				zap.Error(err), zap.String("query", query),
				zap.Bool("instant", h.opts.instant))
			break
		}

		// Determine maxSeries based on either series or datapoints limit. Vector has one datapoint per
		// series and so the datapoint limit behaves the same way as the series one.
		switch {
		case seriesLimit > 0 && datapointsLimit == 0:
			series = seriesLimit
		case seriesLimit == 0 && datapointsLimit > 0:
			series = datapointsLimit
		case seriesLimit == 0 && datapointsLimit == 0:
			// Set max to the actual size if no limits.
			series = len(v)
		default:
			// Take the min of the two limits if both present.
			series = seriesLimit
			if seriesLimit > datapointsLimit {
				series = datapointsLimit
			}
		}

		seriesTotal = len(v)
		limited = series < seriesTotal

		if limited {
			limitedSeries := v[:series]
			res.Value = limitedSeries
			datapoints = len(limitedSeries)
		} else {
			series = seriesTotal
			datapoints = seriesTotal
		}
	case parser.ValueTypeMatrix:
		m, err := res.Matrix()
		if err != nil {
			h.logger.Error("error parsing vector for returned data limits",
				zap.Error(err), zap.String("query", query),
				zap.Bool("instant", h.opts.instant))
			break
		}

		for _, d := range m {
			datapointCount := len(d.Points)
			if fetchOpts.ReturnedSeriesLimit > 0 && series+1 > fetchOpts.ReturnedSeriesLimit {
				limited = true
				break
			}
			if fetchOpts.ReturnedDatapointsLimit > 0 && datapoints+datapointCount > fetchOpts.ReturnedDatapointsLimit {
				limited = true
				break
			}
			series++
			datapoints += datapointCount
		}
		seriesTotal = len(m)

		if series < seriesTotal {
			res.Value = m[:series]
		}
	}

	return limited, native.ReturnedDataLimited{
		Series:      series,
		Datapoints:  datapoints,
		TotalSeries: seriesTotal,
	}
}
