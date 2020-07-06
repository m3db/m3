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

package handleroptions

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
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
	Limits        FetchOptionsBuilderLimitsOptions
	RestrictByTag *storage.RestrictByTag
}

// FetchOptionsBuilderLimitsOptions provides limits options to use when
// creating a fetch options builder.
type FetchOptionsBuilderLimitsOptions struct {
	SeriesLimit       int
	DocsLimit         int
	RequireExhaustive bool
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
func ParseLimit(req *http.Request, header, formValue string, defaultLimit int) (int, error) {
	if str := req.Header.Get(header); str != "" {
		n, err := strconv.Atoi(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return 0, err
		}
		return n, nil
	}

	if str := req.FormValue(formValue); str != "" {
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

// ParseRequireExhaustive parses request limit require exhaustive from header or
// query string.
func ParseRequireExhaustive(req *http.Request, defaultValue bool) (bool, error) {
	if str := req.Header.Get(LimitRequireExhaustiveHeader); str != "" {
		v, err := strconv.ParseBool(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return false, err
		}
		return v, nil
	}

	if str := req.FormValue("requireExhaustive"); str != "" {
		v, err := strconv.ParseBool(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse limit: input=%s, err=%v", str, err)
			return false, err
		}
		return v, nil
	}

	return defaultValue, nil
}

// NewFetchOptions parses an http request into fetch options.
func (b fetchOptionsBuilder) NewFetchOptions(
	req *http.Request,
) (*storage.FetchOptions, *xhttp.ParseError) {
	fetchOpts := storage.NewFetchOptions()

	seriesLimit, err := ParseLimit(req, LimitMaxSeriesHeader, "limit", b.opts.Limits.SeriesLimit)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	fetchOpts.SeriesLimit = seriesLimit

	docsLimit, err := ParseLimit(req, LimitMaxDocsHeader, "docsLimit", b.opts.Limits.DocsLimit)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	fetchOpts.DocsLimit = docsLimit

	requireExhaustive, err := ParseRequireExhaustive(req, b.opts.Limits.RequireExhaustive)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	fetchOpts.RequireExhaustive = requireExhaustive

	if str := req.Header.Get(MetricsTypeHeader); str != "" {
		mt, err := storagemetadata.ParseMetricsType(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse metrics type: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType =
			newOrExistingRestrictQueryOptionsRestrictByType(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType.MetricsType = mt
	}

	if str := req.Header.Get(MetricsStoragePolicyHeader); str != "" {
		sp, err := policy.ParseStoragePolicy(str)
		if err != nil {
			err = fmt.Errorf(
				"could not parse storage policy: input=%s, err=%v", str, err)
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType =
			newOrExistingRestrictQueryOptionsRestrictByType(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByType.StoragePolicy = sp
	}

	if str := req.Header.Get(RestrictByTagsJSONHeader); str != "" {
		// Allow header to override any default restrict by tags config.
		var opts StringTagOptions
		if err := json.Unmarshal([]byte(str), &opts); err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		tagOpts, err := opts.StorageOptions()
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByTag = tagOpts
	} else if defaultTagOpts := b.opts.RestrictByTag; defaultTagOpts != nil {
		// Apply defaults if not overridden by header.
		fetchOpts.RestrictQueryOptions = newOrExistingRestrictQueryOptions(fetchOpts)
		fetchOpts.RestrictQueryOptions.RestrictByTag = defaultTagOpts
	}

	if restrict := fetchOpts.RestrictQueryOptions; restrict != nil {
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

	return fetchOpts, nil
}

func newOrExistingRestrictQueryOptions(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictQueryOptions {
	if v := fetchOpts.RestrictQueryOptions; v != nil {
		return v
	}
	return &storage.RestrictQueryOptions{}
}

func newOrExistingRestrictQueryOptionsRestrictByType(
	fetchOpts *storage.FetchOptions,
) *storage.RestrictByType {
	if v := fetchOpts.RestrictQueryOptions.RestrictByType; v != nil {
		return v
	}
	return &storage.RestrictByType{}
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
