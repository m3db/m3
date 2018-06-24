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
	"fmt"
	"net/http"
	"time"

	"github.com/m3db/m3db/src/coordinator/api/v1/handler"
	"github.com/m3db/m3db/src/coordinator/api/v1/handler/prometheus"
	"github.com/m3db/m3db/src/coordinator/errors"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/util"
)

const (
	endParam    = "end"
	startParam  = "start"
	targetQuery = "target"
	stepParam   = "step"

	formatErrStr = "error parsing param: %s, error: %v"
)

func parseTime(r *http.Request, key string) (time.Time, error) {
	if t := r.FormValue(key); t != "" {
		return util.ParseTimeString(t)
	}

	return time.Time{}, errors.ErrNotFound
}

// nolint: unparam
func parseDuration(r *http.Request, key string) (time.Duration, error) {
	if d := r.FormValue(key); d != "" {
		return time.ParseDuration(d)
	}

	return 0, errors.ErrNotFound
}

// ParseParams parses all params from the GET request
func ParseParams(r *http.Request) (models.RequestParams, *handler.ParseError) {
	params := models.RequestParams{
		Now: time.Now(),
	}

	t, err := prometheus.ParseRequestTimeout(r)
	if err != nil {
		return params, handler.NewParseError(err, http.StatusBadRequest)
	}

	params.Timeout = t
	start, err := parseTime(r, startParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, startParam, err), http.StatusBadRequest)
	}

	params.Start = start
	end, err := parseTime(r, endParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, endParam, err), http.StatusBadRequest)
	}

	params.End = end
	step, err := parseDuration(r, stepParam)
	if err != nil {
		return params, handler.NewParseError(fmt.Errorf(formatErrStr, stepParam, err), http.StatusBadRequest)
	}

	params.Step = step
	target, pErr := parseTarget(r)
	if pErr != nil {
		return params, pErr
	}

	params.Target = target
	return params, nil
}

func parseTarget(r *http.Request) (string, *handler.ParseError) {
	targetQueries, ok := r.URL.Query()[targetQuery]
	if !ok || len(targetQueries) == 0 || targetQueries[0] == "" {
		return "", handler.NewParseError(errors.ErrNoTargetFound, http.StatusBadRequest)
	}

	// TODO: currently, we only support one target at a time
	if len(targetQueries) > 1 {
		return "", handler.NewParseError(errors.ErrBatchQuery, http.StatusBadRequest)
	}

	return targetQueries[0], nil
}
