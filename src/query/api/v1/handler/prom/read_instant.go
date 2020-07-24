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
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage/prometheus"
	"github.com/m3db/m3/src/query/util"

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
	ts, err := util.ParseTimeStringWithDefault(r.FormValue("time"), time.Now())
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
	var resultMetadata block.ResultMetadata
	ctx = context.WithValue(ctx, prometheus.FetchOptionsContextKey, fetchOptions)
	ctx = context.WithValue(ctx, prometheus.BlockResultMetadataKey, &resultMetadata)

	if t := r.FormValue("timeout"); t != "" {
		timeout, err := util.ParseDurationString(t)
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
