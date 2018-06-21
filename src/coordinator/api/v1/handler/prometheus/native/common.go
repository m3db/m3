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

package native

import (
	"time"
	"net/http"

	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/util"
)

const (
	endParam    = "end"
	startParam  = "start"
	targetQuery = "target"
)

func parseTime(r *http.Request, key string) (time.Time, error) {
	if t := r.FormValue(key); t != "" {
		return util.ParseTimeString(t)
	}

	return time.Time{}, errors.ErrHeaderNotFound
}

type RequestParams struct {
	Start   time.Time
	End     time.Time
	Timeout time.Duration
	Target  string
	// Now captures the current time and fixes it throughout the request, we may let people override it in the future
	Now     time.Time
}

// ParseParams parses all params from the GET request
func ParseParams(r *http.Request) (RequestParams, *handler.ParseError) {
	params := RequestParams{
		Now: time.Now(),
	}
	if t, err := prometheus.ParseRequestTimeout(r); err != nil {
		return params, handler.NewParseError(err, http.StatusBadRequest)
	} else {
		params.Timeout = t
	}

	if t, err := parseTime(r, startParam); err != nil {
		return params, handler.NewParseError(err, http.StatusBadRequest)
	} else {
		params.Start = t
	}

	if t, err := parseTime(r, endParam); err != nil {
		return params, handler.NewParseError(err, http.StatusBadRequest)
	} else {
		params.End = t
	}

	if target, err := parseTarget(r); err != nil {
		return params, err
	} else {
		params.Target = target
	}

	return params, nil
}

func parseTarget(r *http.Request) (string, *handler.ParseError) {
	targetQueries, ok := r.URL.Query()[targetQuery]
	if !ok || len(targetQueries) == 0 {
		return "", handler.NewParseError(errors.ErrNoTargetFound, http.StatusBadRequest)
	}

	// TODO: currently, we only support one target at a time
	if len(targetQueries) > 1 {
		return "", handler.NewParseError(errors.ErrBatchQuery, http.StatusBadRequest)
	}

	return targetQueries[0], nil
}
