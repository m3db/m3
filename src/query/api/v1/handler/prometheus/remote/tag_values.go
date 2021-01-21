// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"context"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	// NameReplace is the parameter that gets replaced.
	NameReplace = "name"

	// TagValuesURL is the url for tag values.
	TagValuesURL = handler.RoutePrefixV1 +
		"/label/{" + NameReplace + "}/values"

	// TagValuesHTTPMethod is the HTTP method used with this resource.
	TagValuesHTTPMethod = http.MethodGet
)

// TagValuesHandler represents a handler for search tags endpoint.
type TagValuesHandler struct {
	storage             storage.Storage
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	parseOpts           promql.ParseOptions
	instrumentOpts      instrument.Options
}

// TagValuesResponse is the response that gets returned to the user
type TagValuesResponse struct {
	Results consolidators.CompleteTagsResult `json:"results,omitempty"`
}

// NewTagValuesHandler returns a new instance of handler.
func NewTagValuesHandler(options options.HandlerOptions) http.Handler {
	return &TagValuesHandler{
		storage:             options.Storage(),
		fetchOptionsBuilder: options.FetchOptionsBuilder(),
		parseOpts:           promql.NewParseOptions().SetNowFn(options.NowFn()),
		instrumentOpts:      options.InstrumentOpts(),
	}
}

func (h *TagValuesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), handler.HeaderKey, r.Header)
	logger := logging.WithContext(ctx, h.instrumentOpts)
	w.Header().Set(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	query, err := h.parseTagValuesToQuery(r)
	if err != nil {
		logger.Error("unable to parse tag values to query", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	opts, rErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	result, err := h.storage.CompleteTags(ctx, query, opts)
	if err != nil {
		logger.Error("unable to get tag values", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	handleroptions.AddResponseHeaders(w, result.Metadata, opts)
	// TODO: Support multiple result types
	err = prometheus.RenderTagValuesResultsJSON(w, result)
	if err != nil {
		logger.Error("unable to render tag values", zap.Error(err))
		xhttp.WriteError(w, err)
	}
}

func (h *TagValuesHandler) parseTagValuesToQuery(
	r *http.Request,
) (*storage.CompleteTagsQuery, error) {
	vars := mux.Vars(r)
	name, ok := vars[NameReplace]
	if !ok || len(name) == 0 {
		return nil, xhttp.NewError(errors.ErrNoName, http.StatusBadRequest)
	}

	start, end, err := prometheus.ParseStartAndEnd(r, h.parseOpts)
	if err != nil {
		return nil, err
	}

	nameBytes := []byte(name)
	return &storage.CompleteTagsQuery{
		Start:            start,
		End:              end,
		CompleteNameOnly: false,
		FilterNameTags:   [][]byte{nameBytes},
		TagMatchers: models.Matchers{
			models.Matcher{
				Type: models.MatchField,
				Name: nameBytes,
			},
		},
	}, nil
}
