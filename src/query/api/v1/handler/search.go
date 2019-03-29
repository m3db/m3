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
	store storage.Storage
}

// NewSearchHandler returns a new instance of handler
func NewSearchHandler(storage storage.Storage) http.Handler {
	return &SearchHandler{store: storage}
}

func (h *SearchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context())

	query, rErr := h.parseBody(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}
	opts := h.parseURLParams(r)

	results, err := h.search(r.Context(), query, opts)
	if err != nil {
		logger.Error("unable to fetch data", zap.Any("error", err))
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

func (h *SearchHandler) parseURLParams(r *http.Request) *storage.FetchOptions {
	var (
		limit int
		err   error
	)

	limitRaw := r.URL.Query().Get("limit")
	if limitRaw != "" {
		limit, err = strconv.Atoi(limitRaw)
		if err != nil {
			limit = defaultLimit
		}
	} else {
		limit = defaultLimit
	}

	fetchOptions := newFetchOptions(limit)
	return &fetchOptions
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
