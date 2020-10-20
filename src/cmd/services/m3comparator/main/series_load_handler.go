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

package main

import (
	"net/http"
	"sync"

	"github.com/m3db/m3/src/cmd/services/m3comparator/main/parser"
	"github.com/m3db/m3/src/dbnode/encoding"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

type seriesLoadHandler interface {
	getSeriesIterators(string) (encoding.SeriesIterators, error)
}

type httpSeriesLoadHandler struct {
	sync.RWMutex
	reader   parser.SeriesReader
	iterOpts parser.Options
}

var (
	_ http.Handler      = (*httpSeriesLoadHandler)(nil)
	_ seriesLoadHandler = (*httpSeriesLoadHandler)(nil)
)

// newHTTPSeriesLoadHandler builds a handler that can load series
// to the comparator via an http endpoint.
func newHTTPSeriesLoadHandler(iterOpts parser.Options) *httpSeriesLoadHandler {
	return &httpSeriesLoadHandler{
		iterOpts: iterOpts,
		reader:   parser.NewSeriesReader(iterOpts),
	}
}

func (l *httpSeriesLoadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := l.iterOpts.InstrumentOptions.Logger()
	err := l.serveHTTP(r)
	if err != nil {
		logger.Error("unable to fetch data", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (l *httpSeriesLoadHandler) getSeriesIterators(
	name string) (encoding.SeriesIterators, error) {
	return l.reader.SeriesIterators(name)
}

func (l *httpSeriesLoadHandler) serveHTTP(r *http.Request) error {
	if r.Method == http.MethodDelete {
		l.reader.Clear()
		return nil
	}

	body := r.Body
	defer body.Close()
	return l.reader.Load(body)
}
