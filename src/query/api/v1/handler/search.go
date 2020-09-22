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

package handler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// SearchURL is the url to search for metric ids
	SearchURL = "/search"

	// SearchHTTPMethod is the HTTP method used with this resource.
	SearchHTTPMethod = http.MethodPost

	defaultLimit = 1000
)

// SearchHandler represents a handler for the search endpoint
type SearchHandler struct {
	store               storage.Storage
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder
	instrumentOpts      instrument.Options
}

// NewSearchHandler returns a new instance of handler
func NewSearchHandler(opts options.HandlerOptions) http.Handler {
	return &SearchHandler{
		store:               opts.Storage(),
		fetchOptionsBuilder: opts.FetchOptionsBuilder(),
		instrumentOpts:      opts.InstrumentOpts(),
	}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context(), h.instrumentOpts)

	query, parseBodyErr := h.parseBody(r)
	fetchOpts, parseURLParamsErr := h.parseURLParams(r)
	if err := firstParseError(parseBodyErr, parseURLParamsErr); err != nil {
		logger.Error("unable to parse request", zap.Error(err.Inner()))
		xhttp.Error(w, err.Inner(), err.Code())
		return
	}

	results, err := h.search(r.Context(), query, fetchOpts)
	if err != nil {
		logger.Error("search query error",
			zap.Error(err),
			zap.Any("query", query),
			zap.Any("fetchOpts", fetchOpts))
		xhttp.Error(w, err, http.StatusBadRequest)
		return
	}

	xhttp.WriteJSONResponse(w, results, logger)
}

func (h *SearchHandler) parseBody(r *http.Request) (*storage.FetchQuery, *xhttp.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	var fetchQuery storage.FetchQuery
	if err := json.Unmarshal(body, &fetchQuery); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return &fetchQuery, nil
}

func (h *SearchHandler) parseURLParams(r *http.Request) (*storage.FetchOptions, *xhttp.ParseError) {
	fetchOpts, parseErr := h.fetchOptionsBuilder.NewFetchOptions(r)
	if parseErr != nil {
		return nil, parseErr
	}

	// Parse for series and docs limits as query params.
	// For backwards compat, allow "limit" and "seriesLimit"
	// for the series limit name.
	if str := r.URL.Query().Get("limit"); str != "" {
		var err error
		fetchOpts.SeriesLimit, err = strconv.Atoi(str)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	} else if str := r.URL.Query().Get("seriesLimit"); str != "" {
		var err error
		fetchOpts.SeriesLimit, err = strconv.Atoi(str)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	if str := r.URL.Query().Get("docsLimit"); str != "" {
		var err error
		fetchOpts.DocsLimit, err = strconv.Atoi(str)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	if str := r.URL.Query().Get("requireExhaustive"); str != "" {
		var err error
		fetchOpts.RequireExhaustive, err = strconv.ParseBool(str)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	return fetchOpts, nil
}

func (h *SearchHandler) search(
	ctx context.Context,
	query *storage.FetchQuery,
	opts *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return h.store.SearchSeries(ctx, query, opts)
}

func firstParseError(errs ...*xhttp.ParseError) *xhttp.ParseError {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
