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

	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

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
	fetchOptionsBuilder FetchOptionsBuilder
}

// NewSearchHandler returns a new instance of handler
func NewSearchHandler(
	storage storage.Storage,
	fetchOptionsBuilder FetchOptionsBuilder,
) http.Handler {
	return &SearchHandler{
		store:               storage,
		fetchOptionsBuilder: fetchOptionsBuilder,
	}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context())

	query, parseBodyErr := h.parseBody(r)
	opts, parseURLParamsErr := h.parseURLParams(r)
	// NB(r): Use a loop here to avoid two err handling code paths
	for _, rErr := range []*xhttp.ParseError{parseBodyErr, parseURLParamsErr} {
		logger.Error("unable to parse request", zap.Error(rErr.Inner()))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	results, err := h.search(r.Context(), query, opts)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
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
	fetchOpts, err := h.fetchOptionsBuilder.NewFetchOptions(r)
	if err != nil {
		return nil, err
	}

	if str := r.URL.Query().Get("limit"); str != "" {
		if limit, err := strconv.Atoi(str); err == nil {
			fetchOpts.Limit = limit
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

func newFetchOptions(limit int) storage.FetchOptions {
	return storage.FetchOptions{
		Limit: limit,
	}
}
