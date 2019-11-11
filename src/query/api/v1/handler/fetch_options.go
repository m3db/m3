// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// StepParam is the step parameter.
	StepParam = "step"
	// LookbackParam is the lookback parameter.
	LookbackParam = "lookback"
	maxInt64      = float64(math.MaxInt64)
	minInt64      = float64(math.MinInt64)
)

// FetchOptionsBuilder builds fetch options based on a request and default
// config.
type FetchOptionsBuilder interface {
	// NewFetchOptions parses an http request into fetch options.
	NewFetchOptions(req *http.Request) (*storage.FetchOptions, *xhttp.ParseError)
}

// FetchOptionsBuilderOptions provides options to use when creating a
// fetch options builder.
type FetchOptionsBuilderOptions struct {
	Limit int
}

type fetchOptionsBuilder struct {
	opts FetchOptionsBuilderOptions
}

// NewFetchOptionsBuilder returns a new fetch options builder.
func NewFetchOptionsBuilder(
	opts FetchOptionsBuilderOptions,
) FetchOptionsBuilder {
	return fetchOptionsBuilder{opts: opts}
}

// ParseLimit parses request limit from either header or query string.
func ParseLimit(req *http.Request, defaultLimit int) (int, error) {
	if str := req.Header.Get(LimitMaxSeriesHeader); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return 0, err
		}

		return n, nil
	}

	if str := req.URL.Query().Get("limit"); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return 0, err
		}

		return n, nil
	}

	return defaultLimit, nil
}

// NewFetchOptions parses an http request into fetch options.
func (b fetchOptionsBuilder) NewFetchOptions(
	req *http.Request,
) (*storage.FetchOptions, *xhttp.ParseError) {
	fetchOpts := storage.NewFetchOptions()
	limit, err := ParseLimit(req, b.opts.Limit)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}
	fetchOpts.Limit = limit
	if str := req.Header.Get(MetricsTypeHeader); str != "" {
		mt, err := storage.ParseMetricsType(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse metrics type: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
		fetchOpts.RestrictFetchOptions = newOrExistingRestrictFetchOptions(fetchOpts)
		fetchOpts.RestrictFetchOptions.MetricsType = mt
	}
	if str := req.Header.Get(MetricsStoragePolicyHeader); str != "" {
		sp, err := policy.ParseStoragePolicy(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse storage policy: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
		fetchOpts.RestrictFetchOptions = newOrExistingRestrictFetchOptions(fetchOpts)
		fetchOpts.RestrictFetchOptions.StoragePolicy = sp
	}
	if restrict := fetchOpts.RestrictFetchOptions; restrict != nil {
		if err := restrict.Validate(); err != nil {
			err = fmt.Errorf(
				"could not validate restrict options: err=%v", err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}
	}

	// NB(r): Eventually all query parameters that are common across the
	// implementations should be parsed here so they can be fed to the engine.
	if step, ok, err := ParseStep(req); err != nil {
		err = fmt.Errorf(
			"could not parse step: err=%v", err)
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	} else if ok {
		fetchOpts.Step = step
	}
	if lookback, ok, err := ParseLookbackDuration(req); err != nil {
		err = fmt.Errorf(
			"could not parse lookback: err=%v", err)
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	} else if ok {
		fetchOpts.LookbackDuration = &lookback
	}

	// TODO: IncludeExemplars parsing?

	return fetchOpts, nil
}

func newOrExistingRestrictFetchOptions(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictFetchOptions {
	if v := fetchOpts.RestrictFetchOptions; v != nil {
		return v
	}
	return &storage.RestrictFetchOptions{}
}

// ParseStep parses the step duration for an HTTP request.
func ParseStep(r *http.Request) (time.Duration, bool, error) {
	step := r.FormValue(StepParam)
	if step == "" {
		return 0, false, nil
	}
	value, err := parseStep(r, StepParam)
	if err != nil {
		return 0, false, err
	}
	return value, true, err
}

func parseStep(r *http.Request, key string) (time.Duration, error) {
	step, err := ParseDuration(r, key)
	if err != nil {
		return 0, err
	}
	if step <= 0 {
		return 0, fmt.Errorf("expected postive step size, instead got: %d", step)
	}
	return step, nil
}

// ParseLookbackDuration parses a lookback duration for an HTTP request.
func ParseLookbackDuration(r *http.Request) (time.Duration, bool, error) {
	lookback := r.FormValue(LookbackParam)
	if lookback == "" {
		return 0, false, nil
	}

	if lookback == StepParam {
		// Use the step size as lookback.
		step, err := parseStep(r, StepParam)
		if err != nil {
			return 0, false, err
		}
		return step, true, nil
	}

	// Otherwise it is specified as duration value.
	value, err := parseStep(r, LookbackParam)
	if err != nil {
		return 0, false, err
	}

	return value, true, nil
}

// ParseDuration parses a duration HTTP parameter.
// nolint: unparam
func ParseDuration(r *http.Request, key string) (time.Duration, error) {
	str := strings.TrimSpace(r.FormValue(key))
	if str == "" {
		return 0, errors.ErrNotFound
	}

	value, durationErr := time.ParseDuration(str)
	if durationErr == nil {
		return value, nil
	}

	// Try parsing as a float value specifying seconds, the Prometheus default.
	seconds, floatErr := strconv.ParseFloat(str, 64)
	if floatErr == nil {
		ts := seconds * float64(time.Second)
		if ts > maxInt64 || ts < minInt64 {
			return 0, fmt.Errorf("cannot parse duration='%s': as_float_err="+
				"int64 overflow after float conversion", str)
		}

		return time.Duration(ts), nil
	}

	return 0, fmt.Errorf("cannot parse duration='%s': as_duration_err=%s, as_float_err=%s",
		str, durationErr, floatErr)
}
